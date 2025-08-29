from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone, timedelta
import io


def main(args, settings):
    arg_archive_file = args.get('<archive_file>', None)
    if not arg_archive_file:
        print("Error: <archive_file> argument is required")
        return False

    azure_storage_container_name = settings.get('AZURE_STORAGE_CONTAINER_NAME')
    azure_storage_connection_string = settings.get('AZURE_STORAGE_CONNECTION_STRING')

    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
        container_client = blob_service_client.get_container_client(azure_storage_container_name)

        # List all blobs (with versions if available)
        blob_versions = list(
            container_client.list_blobs(
                name_starts_with=arg_archive_file,
                include=["versions"]
            )
        )

        if not blob_versions:
            raise Exception(f"No blob found for {arg_archive_file}")

        # Sort by last modified time (descending)
        blob_versions.sort(key=lambda b: b['last_modified'], reverse=True)
        latest_version = blob_versions[0]

        version_id = latest_version.get('version_id')
        last_modified = latest_version['last_modified']

        # Check backup freshness (example: max 24 hours old)
        now = datetime.now(timezone.utc)
        max_age = timedelta(hours=24)
        if (now - last_modified) > max_age:
            raise Exception(
                f"Latest backup is too old! Last modified: {last_modified}, "
                f"current time: {now}, allowed max age: {max_age}"
            )

        # Pick the correct blob client (version-aware if available)
        if version_id:
            blob_client = container_client.get_blob_client(
                blob=arg_archive_file,
                version_id=version_id
            )
            print(f"Downloading latest version of {arg_archive_file}")
            print(f"  Last Modified: {last_modified}")
            print(f"  Version ID: {version_id}")
        else:
            blob_client = container_client.get_blob_client(blob=arg_archive_file)
            print(f"Downloading current blob (no versioning enabled): {arg_archive_file}")
            print(f"  Last Modified: {last_modified}")

        # Download the blob
        azure_storage_bytes = blob_client.download_blob().readall()
        azure_storage_data = io.BytesIO(azure_storage_bytes)

        print("Download from Azure Storage was successful")

    except Exception as e:
        print("Error:", str(e))
        return False

    return azure_storage_data
