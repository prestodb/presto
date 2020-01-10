=============
Release 0.231
=============

SPI Changes
___________
* Change ``ConnectorMetadata#commitPartition`` into async operation, and rename it to ``ConnectorMetadata#commitPartitionAsync``.

Raptor Changes
______________
* Change `storage.data-directory` from path to URI. For existing deployment on local flash, a scheme header "file://" should be added to the original config value.
* Change error code name `RAPTOR_LOCAL_FILE_SYSTEM_ERROR` to `RAPTOR_FILE_SYSTEM_ERROR`.
* Add table_supports_delta_delete property in Raptor to allow deletion happening in background. DELETE queries in Raptor can now delete data logically but relying on compactors to delete physical data.
