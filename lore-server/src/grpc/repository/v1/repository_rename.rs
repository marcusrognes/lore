// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::KeyType;
use lore_proto::lore::repository::v1::RepositoryRenameRequest;
use lore_proto::lore::repository::v1::RepositoryRenameResponse;
use lore_proto::lore::repository::v1::repository_rename_request::Query;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_storage::hash;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;

use super::record::build_repository;
use super::repository_get::repository_load_id;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::ServerResultExt;
use crate::grpc::extract_authorization_header;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_user_id;
use crate::grpc::get_write_token;
use crate::grpc::no_repository_access_status;
use crate::grpc::warn_error_to_status;
use crate::util::setup_execution;

/// `lore.repository.v1.RepositoryService.RepositoryRename` handler.
///
/// A repository's name lives in two places: the canonical `name` in its
/// metadata blob, and the name → id mapping the resolver reads. They have to
/// move together — a mapping that disagrees with the blob is deleted on the
/// next resolve — which is why `name` is read-only on the generic
/// `RepositoryMetadataSet` path and why this operation exists instead.
///
/// The order is metadata, then new mapping, then old mapping, so a run that
/// dies part way leaves the repository resolvable and a re-run finishes the
/// job: `RepositoryGet` by id already repairs the mapping for whatever name
/// the metadata carries. Re-issuing a rename that has already happened is a
/// no-op that still repairs the mapping.
///
/// Only repositories are renameable. A branch id is derived from its name, so
/// renaming one would change its identity; a repository id is an independent
/// UUIDv7 and nothing structural hangs off the name.
#[tracing::instrument(name = "RepositoryRename::v1::handle", skip_all)]
pub async fn handler(
    request: Request<RepositoryRenameRequest>,
    authorizer: Arc<dyn RepositoryAuthorizer>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
) -> Result<Response<RepositoryRenameResponse>, Status> {
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let authorization = extract_authorization_header(&request);
    let req = request.into_inner();

    let Some(query) = req.query else {
        return Err(Status::invalid_argument(
            "RepositoryRenameRequest.query must be set (id or name)",
        ));
    };

    let new_name = req.new_name;
    if !repository::is_valid_name(new_name.as_str()) {
        return Err(Status::invalid_argument(format!(
            "Invalid repository name {new_name}"
        )));
    }

    let execution = setup_execution(module_path!(), correlation_id, user_id);
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        RepositoryId::default(),
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            let id = match query {
                Query::Id(id) => Context::from(id).into(),
                Query::Name(name) => repository::id_from_name(repository.clone(), name.as_str())
                    .await
                    .filter_slow_down()?
                    .map_err(|_err| Status::not_found(format!("Repository {name} not found")))?,
            };
            if id == RepositoryId::default() {
                return Err(Status::invalid_argument("Missing repository id"));
            }

            // Renaming is a write, so it takes the write authorization the
            // metadata mutations take rather than the read-only query check.
            authorizer
                .check_repository_access(authorization, id)
                .await
                .map_err(|_err| no_repository_access_status())?;

            let repository = Arc::new(repository.to_server_context(id));
            let (metadata, metadata_hash) = repository_load_id(repository.clone(), id, None, None)
                .await
                .filter_slow_down()?
                .map_err(|_err| Status::not_found(format!("Repository {id} not found")))?;

            let old_name = metadata.name.clone();
            let name_repository = Arc::new(repository.to_server_context(RepositoryId::default()));

            // A name already pointing somewhere else is a collision, the same
            // way `RepositoryCreate` treats one. Pointing at this repository
            // is the half-finished rename this call is here to finish.
            if old_name != new_name
                && let Ok(taken) =
                    repository::id_from_name(name_repository.clone(), new_name.as_str()).await
                && taken != id
                && taken != RepositoryId::default()
            {
                return Err(Status::already_exists(format!(
                    "Repository {new_name} already exists"
                )));
            }

            let metadata_hash = if old_name == new_name {
                metadata_hash
            } else {
                install_name(repository.clone(), id, &metadata, metadata_hash, &new_name).await?
            };

            // Registering the new mapping is idempotent, so it runs even when
            // the metadata already carried the new name: that is exactly the
            // state a rename interrupted between these two steps leaves behind.
            repository::store_name_to_id(name_repository.clone(), new_name.as_str(), id)
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to store repository name mapping: {err}"))
                })?;

            if old_name != new_name {
                repository::delete_name_to_id(name_repository, old_name.as_str())
                    .await
                    .filter_slow_down()?
                    .warn_map_err(|err| {
                        Status::internal(format!(
                            "Failed to delete previous repository name mapping: {err}"
                        ))
                    })?;
                info!(%id, "Renamed repository {old_name} to {new_name}");
            }

            let renamed = lore_revision::repository::RepositoryMetadata {
                name: new_name,
                ..metadata
            };
            Ok(Response::new(RepositoryRenameResponse {
                repository: Some(build_repository(id, &renamed, metadata_hash)),
            }))
        })
        .await
}

/// Write a metadata blob carrying `new_name` and swap the repository's
/// metadata pointer onto it, answering the new pointer.
///
/// The swap is the same compare-and-swap `RepositoryMetadataSet` performs, so
/// a metadata write that lands between the load above and this call is lost
/// rather than overwritten — reported as `Aborted` for the caller to retry.
async fn install_name(
    repository: Arc<RepositoryContext>,
    id: RepositoryId,
    metadata: &lore_revision::repository::RepositoryMetadata,
    expected: lore_base::types::Hash,
    new_name: &str,
) -> Result<lore_base::types::Hash, Status> {
    let renamed = lore_revision::repository::RepositoryMetadata {
        name: new_name.to_string(),
        ..metadata.clone()
    };
    let updated = repository::metadata_store(repository.clone(), renamed)
        .await
        .filter_slow_down()?
        .map_err(|err| {
            warn_error_to_status(&err, |err| {
                Status::internal(format!("failed to store renamed metadata: {err}"))
            })
        })?;

    let metadata_key = hash::hash_function_arg(
        repository::SALT_LORE,
        repository::METADATA,
        hex::encode(Context::from(id).data()).as_str(),
    );
    let write_token = get_write_token();
    let previous = repository
        .write_mutable_store(&write_token)
        .compare_and_swap(
            id,
            metadata_key,
            expected,
            updated,
            KeyType::RepositoryMetadata,
        )
        .await
        .filter_slow_down()?
        .map_err(|err| {
            warn_error_to_status(&err, |err| {
                Status::internal(format!("failed to update metadata: {err}"))
            })
        })?;

    if previous != expected {
        return Err(Status::aborted(
            "repository metadata was modified concurrently",
        ));
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::Hash;
    use lore_proto::lore::repository::v1::repository_rename_request::Query;
    use lore_revision::branch;
    use lore_revision::branch::BranchLatestStatus;
    use lore_revision::lore::RepositoryId;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryContext;
    use lore_revision::repository::RepositoryMetadata;
    use rand::random;

    use super::super::repository_get::repository_load_id;
    use super::super::repository_get::repository_load_name;
    use super::*;
    use crate::authnz::repository_authorizer::AllowAllRepositoryAuthorizer;
    use crate::store::test_store_create;

    /// A repository as `RepositoryCreate` would leave it — metadata blob,
    /// metadata pointer, name → id mapping — plus a branch head, so a rename
    /// can be checked against history it must not move.
    async fn seed_repository(
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        id: RepositoryId,
        name: &str,
        branch: lore_revision::lore::BranchId,
        tip: Hash,
    ) {
        let repository = Arc::new(RepositoryContext::new_server_context(
            immutable_store,
            mutable_store,
            id,
        ));
        let metadata_hash = repository::metadata_store(
            repository.clone(),
            RepositoryMetadata {
                name: name.to_string(),
                description: "seeded".into(),
                default_branch: branch,
                default_branch_name: "main".into(),
                creator: "alice".into(),
                created: 100,
            },
        )
        .await
        .expect("Failed to store repository metadata");
        repository::metadata_store_hash(repository.clone(), metadata_hash)
            .await
            .expect("Failed to store repository metadata hash");
        repository::store_name_to_id(repository.clone(), name, id)
            .await
            .expect("Failed to store repository name to id mapping");
        branch::store_latest(
            repository,
            branch,
            Hash::default(),
            tip,
            BranchLatestStatus::Convergent,
        )
        .await
        .expect("Failed to store branch head");
    }

    fn rename_request(query: Query, new_name: &str) -> Request<RepositoryRenameRequest> {
        Request::new(RepositoryRenameRequest {
            query: Some(query),
            new_name: new_name.to_string(),
        })
    }

    /// A rename has to move both halves of the identity together: the canonical
    /// `name` in the metadata blob, and the name → id mapping the resolver reads.
    /// Everything else — the id, the branch head, the revision graph — is keyed
    /// off the id and must come through untouched.
    #[tokio::test]
    async fn a_rename_moves_the_name_and_leaves_history_alone() {
        let id = random::<RepositoryId>();
        let branch = random::<lore_revision::lore::BranchId>();
        let tip = Hash::from([0x33u8; 32]);
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            seed_repository(
                immutable_store.clone(),
                mutable_store.clone(),
                id,
                "alpha/one",
                branch,
                tip,
            )
            .await;

            let response = handler(
                rename_request(Query::Name("alpha/one".into()), "beta/one"),
                Arc::new(AllowAllRepositoryAuthorizer),
                immutable_store.clone(),
                mutable_store.clone(),
            )
            .await
            .expect("rename should succeed")
            .into_inner()
            .repository
            .expect("rename response should carry the repository");

            assert_eq!(response.name, "beta/one");
            assert_eq!(response.id, bytes::Bytes::from(id));

            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable_store,
                mutable_store,
                RepositoryId::default(),
            ));

            let (resolved_id, metadata, _hash) =
                repository_load_name(repository.clone(), "beta/one", None, None)
                    .await
                    .expect("the new name must resolve");
            assert_eq!(resolved_id, id);
            assert_eq!(metadata.name, "beta/one");

            assert!(
                repository_load_name(repository.clone(), "alpha/one", None, None)
                    .await
                    .is_err(),
                "a rename is hard: the old name stops resolving"
            );

            let (metadata, _hash) = repository_load_id(repository.clone(), id, None, None)
                .await
                .expect("the id must still resolve");
            assert_eq!(metadata.name, "beta/one");

            let head = branch::load_latest(Arc::new(repository.to_server_context(id)), branch)
                .await
                .expect("the branch head must survive a rename");
            assert_eq!(head, tip, "a rename must not move the revision graph");
        }))
        .await;
    }

    /// A rename that fails part way is retried by re-issuing it, so a second
    /// run has to be a no-op rather than an error. Addressing by id is what
    /// makes the retry possible at all: after the first run the old name is
    /// gone, and the id is the only handle left.
    #[tokio::test]
    async fn a_repeated_rename_is_a_no_op() {
        let id = random::<RepositoryId>();
        let branch = random::<lore_revision::lore::BranchId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            seed_repository(
                immutable_store.clone(),
                mutable_store.clone(),
                id,
                "alpha/one",
                branch,
                Hash::from([0x44u8; 32]),
            )
            .await;

            for attempt in 0..2 {
                handler(
                    rename_request(Query::Id(id.into()), "beta/one"),
                    Arc::new(AllowAllRepositoryAuthorizer),
                    immutable_store.clone(),
                    mutable_store.clone(),
                )
                .await
                .unwrap_or_else(|err| panic!("rename attempt {attempt} should succeed: {err}"));
            }

            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable_store,
                mutable_store,
                RepositoryId::default(),
            ));

            let (resolved_id, metadata, _hash) =
                repository_load_name(repository.clone(), "beta/one", None, None)
                    .await
                    .expect("the new name must still resolve after a repeat");
            assert_eq!(resolved_id, id);
            assert_eq!(metadata.name, "beta/one");

            assert!(
                repository_load_name(repository, "alpha/one", None, None)
                    .await
                    .is_err(),
                "the old name must stay gone"
            );
        }))
        .await;
    }
}
