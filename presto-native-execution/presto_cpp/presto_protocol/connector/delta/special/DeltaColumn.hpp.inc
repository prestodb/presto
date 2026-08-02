struct DeltaColumn {
  std::shared_ptr<int64_t> id = {};
  std::shared_ptr<std::string> physicalName = {};
  std::string logicalName = {};
  std::string type = {};
  bool nullable = {};
  bool partition = {};
};
