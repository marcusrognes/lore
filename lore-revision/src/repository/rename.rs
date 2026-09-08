// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;

use lore_error_set::prelude::*;

use super::RepositoryError;
use crate::lore::RepositoryId;
use crate::lore::execution_context;
use crate::protocol;
use crate::repository;

/// Rename the repository `repository_url` names to `new_name`.
///
/// The url addresses the repository by its current name (or by its id); the
/// rename itself goes out by id, so re-issuing it after a partial rename
/// still lands even though the old name has stopped resolving.
pub async fn rename(
    repository_url: &str,
    new_name: &str,
    identity: &str,
) -> Result<(), RepositoryError> {
    // Same addressing as `delete`: a full URL, or a bare name or ID naming a repository on
    // the remote this working copy already points at. Only a scheme separates the two, since
    // `is_valid_name` permits scoped names like `org/project` and a slash therefore does not
    // mean the first segment is a host.
    let (remote_url, name) = if repository_url.contains("://") {
        repository::parse_url(repository_url, false)?
    } else {
        let context = execution_context();
        let repository_path = context.globals().repository_path();
        let remote_url = repository::load_repository_config(repository_path)
            .ok()
            .and_then(|config| config.remote_url)
            .unwrap_or_default();
        if remote_url.is_empty() {
            return Err(RepositoryError::from(crate::errors::NoRemote));
        }
        (remote_url, repository_url.to_string())
    };

    if !repository::is_valid_name(new_name) {
        return Err(RepositoryError::internal(
            "Invalid repository name, can only contain alphanumerical characters and separators /-_",
        ));
    }

    let connection = protocol::connect(
        remote_url.as_str(),
        identity,
        RepositoryId::default(), /* No repository */
    )
    .await
    .forward_with::<RepositoryError, _>(|| {
        format!("Failed to connect to remote repository {remote_url}")
    })?;

    let repository_service = connection
        .repository()
        .await
        .forward_with::<RepositoryError, _>(|| {
            format!("Failed to connect to remote repository {remote_url}")
        })?;

    let mut id = RepositoryId::from_str(name.as_str()).unwrap_or_default();

    if id.is_zero() {
        let data = repository_service
            .query(None, Some(name.as_str()))
            .await
            .forward::<RepositoryError>("Repository not found")?;
        id = data.id;
    }

    if !execution_context().globals().dry_run() {
        repository_service
            .rename(id, new_name)
            .await
            .forward::<RepositoryError>("Failed to rename repository")?;
    }

    Ok(())
}
