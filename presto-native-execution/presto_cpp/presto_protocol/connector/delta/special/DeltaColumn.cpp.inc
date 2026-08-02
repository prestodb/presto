using namespace facebook::presto::protocol;

void to_json(json& j, const DeltaColumn& p) {
  j = json::object();
  if (p.id) {
    to_json_key(j, "id", *p.id, "DeltaColumn", "int64_t", "id");
  }
  if (p.physicalName) {
    to_json_key(
        j,
        "physicalName",
        *p.physicalName,
        "DeltaColumn",
        "std::string",
        "physicalName");
  }
  to_json_key(
      j, "logicalName", p.logicalName, "DeltaColumn", "std::string", "logicalName");
  to_json_key(j, "type", p.type, "DeltaColumn", "std::string", "type");
  to_json_key(j, "nullable", p.nullable, "DeltaColumn", "bool", "nullable");
  to_json_key(j, "partition", p.partition, "DeltaColumn", "bool", "partition");
}

void from_json(const json& j, DeltaColumn& p) {
  // Optional fields - only deserialize if present
  if (j.contains("id")) {
    p.id = std::make_shared<int64_t>();
    from_json_key(j, "id", *p.id, "DeltaColumn", "int64_t", "id");
  }
  if (j.contains("physicalName")) {
    p.physicalName = std::make_shared<std::string>();
    from_json_key(
        j,
        "physicalName",
        *p.physicalName,
        "DeltaColumn",
        "std::string",
        "physicalName");
  }
  // Required fields
  from_json_key(
      j, "logicalName", p.logicalName, "DeltaColumn", "std::string", "logicalName");
  from_json_key(j, "type", p.type, "DeltaColumn", "std::string", "type");
  from_json_key(j, "nullable", p.nullable, "DeltaColumn", "bool", "nullable");
  from_json_key(
      j, "partition", p.partition, "DeltaColumn", "bool", "partition");
}
