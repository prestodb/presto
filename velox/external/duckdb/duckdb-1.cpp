// See https://raw.githubusercontent.com/duckdb/duckdb/master/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif






























namespace duckdb {

string SimilarCatalogEntry::GetQualifiedName() const {
	D_ASSERT(Found());

	return schema->name + "." + name;
}

Catalog::Catalog(DatabaseInstance &db)
    : db(db), schemas(make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this))),
      dependency_manager(make_unique<DependencyManager>(*this)) {
	catalog_version = 0;
}
Catalog::~Catalog() {
}

Catalog &Catalog::GetCatalog(ClientContext &context) {
	return context.db->GetCatalog();
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto schema = GetSchema(context, info->base->schema);
	return CreateTable(context, schema, info);
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info) {
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));
	return CreateTable(context, bound_info.get());
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, SchemaCatalogEntry *schema, BoundCreateTableInfo *info) {
	return schema->CreateTable(context, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateView(context, schema, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info) {
	return schema->CreateView(context, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateSequence(context, schema, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateType(context, schema, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, SchemaCatalogEntry *schema, CreateSequenceInfo *info) {
	return schema->CreateSequence(context, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, SchemaCatalogEntry *schema, CreateTypeInfo *info) {
	return schema->CreateType(context, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateTableFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                           CreateTableFunctionInfo *info) {
	return schema->CreateTableFunction(context, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCopyFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                          CreateCopyFunctionInfo *info) {
	return schema->CreateCopyFunction(context, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePragmaFunction(context, schema, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                            CreatePragmaFunctionInfo *info) {
	return schema->CreatePragmaFunction(context, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	return schema->CreateFunction(context, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCollation(context, schema, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, SchemaCatalogEntry *schema, CreateCollationInfo *info) {
	return schema->CreateCollation(context, info);
}

CatalogEntry *Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo *info) {
	D_ASSERT(!info->schema.empty());
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema);
	}

	unordered_set<CatalogEntry *> dependencies;
	auto entry = make_unique<SchemaCatalogEntry>(this, info->schema, info->internal);
	auto result = entry.get();
	if (!schemas->CreateEntry(context, info->schema, move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("Schema with name %s already exists!", info->schema);
		} else {
			D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		}
		return nullptr;
	}
	return result;
}

void Catalog::DropSchema(ClientContext &context, DropInfo *info) {
	D_ASSERT(!info->name.empty());
	ModifyCatalog();
	if (!schemas->DropEntry(context, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name);
		}
	}
}

void Catalog::DropEntry(ClientContext &context, DropInfo *info) {
	ModifyCatalog();
	if (info->type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	auto lookup = LookupEntry(context, info->type, info->schema, info->name, info->if_exists);
	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return AddFunction(context, schema, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	return schema->AddFunction(context, info);
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &schema_name, bool if_exists,
                                       QueryErrorContext error_context) {
	D_ASSERT(!schema_name.empty());
	if (schema_name == TEMP_SCHEMA) {
		return ClientData::Get(context).temporary_objects.get();
	}
	auto entry = schemas->GetEntry(context, schema_name);
	if (!entry && !if_exists) {
		throw CatalogException(error_context.FormatError("Schema with name %s does not exist!", schema_name));
	}
	return (SchemaCatalogEntry *)entry;
}

void Catalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	// create all default schemas first
	schemas->Scan(context, [&](CatalogEntry *entry) { callback(entry); });
}

SimilarCatalogEntry Catalog::SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
                                                   const vector<SchemaCatalogEntry *> &schemas) {

	vector<CatalogSet *> sets;
	std::transform(schemas.begin(), schemas.end(), std::back_inserter(sets),
	               [type](SchemaCatalogEntry *s) -> CatalogSet * { return &s->GetCatalogSet(type); });
	pair<string, idx_t> most_similar {"", (idx_t)-1};
	SchemaCatalogEntry *schema_of_most_similar = nullptr;
	for (auto schema : schemas) {
		auto entry = schema->GetCatalogSet(type).SimilarEntry(context, entry_name);
		if (!entry.first.empty() && (most_similar.first.empty() || most_similar.second > entry.second)) {
			most_similar = entry;
			schema_of_most_similar = schema;
		}
	}

	return {most_similar.first, most_similar.second, schema_of_most_similar};
}

CatalogException Catalog::CreateMissingEntryException(ClientContext &context, const string &entry_name,
                                                      CatalogType type, const vector<SchemaCatalogEntry *> &schemas,
                                                      QueryErrorContext error_context) {
	auto entry = SimilarEntryInSchemas(context, entry_name, type, schemas);

	vector<SchemaCatalogEntry *> unseen_schemas;
	this->schemas->Scan([&schemas, &unseen_schemas](CatalogEntry *entry) {
		auto schema_entry = (SchemaCatalogEntry *)entry;
		if (std::find(schemas.begin(), schemas.end(), schema_entry) == schemas.end()) {
			unseen_schemas.emplace_back(schema_entry);
		}
	});
	auto unseen_entry = SimilarEntryInSchemas(context, entry_name, type, unseen_schemas);

	string did_you_mean;
	if (unseen_entry.Found() && unseen_entry.distance < entry.distance) {
		did_you_mean = "\nDid you mean \"" + unseen_entry.GetQualifiedName() + "\"?";
	} else if (entry.Found()) {
		did_you_mean = "\nDid you mean \"" + entry.name + "\"?";
	}

	return CatalogException(error_context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
	                                                  entry_name, did_you_mean));
}

CatalogEntryLookup Catalog::LookupEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                        const string &name, bool if_exists, QueryErrorContext error_context) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists, error_context);
		if (!schema) {
			D_ASSERT(if_exists);
			return {nullptr, nullptr};
		}

		auto entry = schema->GetCatalogSet(type).GetEntry(context, name);
		if (!entry && !if_exists) {
			throw CreateMissingEntryException(context, name, type, {schema}, error_context);
		}

		return {schema, entry};
	}

	const auto &paths = ClientData::Get(context).catalog_search_path->Get();
	for (const auto &path : paths) {
		auto lookup = LookupEntry(context, type, path, name, true, error_context);
		if (lookup.Found()) {
			return lookup;
		}
	}

	if (!if_exists) {
		vector<SchemaCatalogEntry *> schemas;
		for (const auto &path : paths) {
			auto schema = GetSchema(context, path, true);
			if (schema) {
				schemas.emplace_back(schema);
			}
		}

		throw CreateMissingEntryException(context, name, type, schemas, error_context);
	}

	return {nullptr, nullptr};
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema, const string &name) {
	vector<CatalogType> entry_types {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY};

	for (auto entry_type : entry_types) {
		CatalogEntry *result = GetEntry(context, entry_type, schema, name, true);
		if (result != nullptr) {
			return result;
		}
	}

	throw CatalogException("CatalogElement \"%s.%s\" does not exist!", schema, name);
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema_name, const string &name,
                                bool if_exists, QueryErrorContext error_context) {
	return LookupEntry(context, type, schema_name, name, if_exists, error_context).entry;
}

template <>
TableCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                     bool if_exists, QueryErrorContext error_context) {
	auto entry = GetEntry(context, CatalogType::TABLE_ENTRY, schema_name, name, if_exists);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::TABLE_ENTRY) {
		throw CatalogException(error_context.FormatError("%s is not a table", name));
	}
	return (TableCatalogEntry *)entry;
}

template <>
SequenceCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                        bool if_exists, QueryErrorContext error_context) {
	return (SequenceCatalogEntry *)GetEntry(context, CatalogType::SEQUENCE_ENTRY, schema_name, name, if_exists,
	                                        error_context);
}

template <>
TableFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                             bool if_exists, QueryErrorContext error_context) {
	return (TableFunctionCatalogEntry *)GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, schema_name, name,
	                                             if_exists, error_context);
}

template <>
CopyFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                            bool if_exists, QueryErrorContext error_context) {
	return (CopyFunctionCatalogEntry *)GetEntry(context, CatalogType::COPY_FUNCTION_ENTRY, schema_name, name, if_exists,
	                                            error_context);
}

template <>
PragmaFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                              bool if_exists, QueryErrorContext error_context) {
	return (PragmaFunctionCatalogEntry *)GetEntry(context, CatalogType::PRAGMA_FUNCTION_ENTRY, schema_name, name,
	                                              if_exists, error_context);
}

template <>
AggregateFunctionCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                                 bool if_exists, QueryErrorContext error_context) {
	auto entry = GetEntry(context, CatalogType::AGGREGATE_FUNCTION_ENTRY, schema_name, name, if_exists, error_context);
	if (entry->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw CatalogException(error_context.FormatError("%s is not an aggregate function", name));
	}
	return (AggregateFunctionCatalogEntry *)entry;
}

template <>
CollateCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                       bool if_exists, QueryErrorContext error_context) {
	return (CollateCatalogEntry *)GetEntry(context, CatalogType::COLLATION_ENTRY, schema_name, name, if_exists,
	                                       error_context);
}

template <>
TypeCatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema_name, const string &name,
                                    bool if_exists, QueryErrorContext error_context) {
	return (TypeCatalogEntry *)GetEntry(context, CatalogType::TYPE_ENTRY, schema_name, name, if_exists, error_context);
}

LogicalType Catalog::GetType(ClientContext &context, const string &schema, const string &name) {
	auto user_type_catalog = GetEntry<TypeCatalogEntry>(context, schema, name);
	auto result_type = user_type_catalog->user_type;
	LogicalType::SetCatalog(result_type, user_type_catalog);
	return result_type;
}

void Catalog::Alter(ClientContext &context, AlterInfo *info) {
	ModifyCatalog();
	auto lookup = LookupEntry(context, info->GetCatalogType(), info->schema, info->name);
	D_ASSERT(lookup.Found()); // It must have thrown otherwise.
	return lookup.schema->Alter(context, info);
}

idx_t Catalog::GetCatalogVersion() {
	return catalog_version;
}

idx_t Catalog::ModifyCatalog() {
	return catalog_version++;
}

} // namespace duckdb





namespace duckdb {

ColumnDependencyManager::ColumnDependencyManager() {
}

ColumnDependencyManager::~ColumnDependencyManager() {
}

void ColumnDependencyManager::AddGeneratedColumn(const ColumnDefinition &column,
                                                 const case_insensitive_map_t<column_t> &name_map) {
	D_ASSERT(column.Generated());
	vector<string> referenced_columns;
	column.GetListOfDependencies(referenced_columns);
	vector<column_t> indices;
	for (auto &col : referenced_columns) {
		auto entry = name_map.find(col);
		if (entry == name_map.end()) {
			throw InvalidInputException("Referenced column \"%s\" was not found in the table", col);
		}
		indices.push_back(entry->second);
	}
	return AddGeneratedColumn(column.Oid(), indices);
}

void ColumnDependencyManager::AddGeneratedColumn(column_t index, const vector<column_t> &indices, bool root) {
	if (indices.empty()) {
		return;
	}
	auto &list = dependents_map[index];
	// Create a link between the dependencies
	for (auto &dep : indices) {
		// Add this column as a dependency of the new column
		list.insert(dep);
		// Add the new column as a dependent of the column
		dependencies_map[dep].insert(index);
		// Inherit the dependencies
		if (HasDependencies(dep)) {
			auto &inherited_deps = dependents_map[dep];
			D_ASSERT(!inherited_deps.empty());
			for (auto &inherited_dep : inherited_deps) {
				list.insert(inherited_dep);
				dependencies_map[inherited_dep].insert(index);
			}
		}
		if (!root) {
			continue;
		}
		direct_dependencies[index].insert(dep);
	}
	if (!HasDependents(index)) {
		return;
	}
	auto &dependents = dependencies_map[index];
	if (dependents.count(index)) {
		throw InvalidInputException("Circular dependency encountered when resolving generated column expressions");
	}
	// Also let the dependents of this generated column inherit the dependencies
	for (auto &dependent : dependents) {
		AddGeneratedColumn(dependent, indices, false);
	}
}

vector<column_t> ColumnDependencyManager::RemoveColumn(column_t index, column_t column_amount) {
	// Always add the initial column
	deleted_columns.insert(index);

	RemoveGeneratedColumn(index);
	RemoveStandardColumn(index);

	// Clean up the internal list
	vector<column_t> new_indices = CleanupInternals(column_amount);
	D_ASSERT(deleted_columns.empty());
	return new_indices;
}

bool ColumnDependencyManager::IsDependencyOf(column_t gcol, column_t col) const {
	auto entry = dependents_map.find(gcol);
	if (entry == dependents_map.end()) {
		return false;
	}
	auto &list = entry->second;
	return list.count(col);
}

bool ColumnDependencyManager::HasDependencies(column_t index) const {
	auto entry = dependents_map.find(index);
	if (entry == dependents_map.end()) {
		return false;
	}
	return true;
}

const unordered_set<column_t> &ColumnDependencyManager::GetDependencies(column_t index) const {
	auto entry = dependents_map.find(index);
	D_ASSERT(entry != dependents_map.end());
	return entry->second;
}

bool ColumnDependencyManager::HasDependents(column_t index) const {
	auto entry = dependencies_map.find(index);
	if (entry == dependencies_map.end()) {
		return false;
	}
	return true;
}

const unordered_set<column_t> &ColumnDependencyManager::GetDependents(column_t index) const {
	auto entry = dependencies_map.find(index);
	D_ASSERT(entry != dependencies_map.end());
	return entry->second;
}

void ColumnDependencyManager::RemoveStandardColumn(column_t index) {
	if (!HasDependents(index)) {
		return;
	}
	auto dependents = dependencies_map[index];
	for (auto &gcol : dependents) {
		// If index is a direct dependency of gcol, remove it from the list
		if (direct_dependencies.find(gcol) != direct_dependencies.end()) {
			direct_dependencies[gcol].erase(index);
		}
		RemoveGeneratedColumn(gcol);
	}
	// Remove this column from the dependencies map
	dependencies_map.erase(index);
}

void ColumnDependencyManager::RemoveGeneratedColumn(column_t index) {
	deleted_columns.insert(index);
	if (!HasDependencies(index)) {
		return;
	}
	auto &dependencies = dependents_map[index];
	for (auto &col : dependencies) {
		// Remove this generated column from the list of this column
		auto &col_dependents = dependencies_map[col];
		D_ASSERT(col_dependents.count(index));
		col_dependents.erase(index);
		// If the resulting list is empty, remove the column from the dependencies map altogether
		if (col_dependents.empty()) {
			dependencies_map.erase(col);
		}
	}
	// Remove this column from the dependents_map map
	dependents_map.erase(index);
}

void ColumnDependencyManager::AdjustSingle(column_t idx, idx_t offset) {
	D_ASSERT(idx >= offset);
	column_t new_idx = idx - offset;
	// Adjust this index in the dependents of this column
	bool has_dependents = HasDependents(idx);
	bool has_dependencies = HasDependencies(idx);

	if (has_dependents) {
		auto &dependents = GetDependents(idx);
		for (auto &dep : dependents) {
			auto &dep_dependencies = dependents_map[dep];
			dep_dependencies.erase(idx);
			D_ASSERT(!dep_dependencies.count(new_idx));
			dep_dependencies.insert(new_idx);
		}
	}
	if (has_dependencies) {
		auto &dependencies = GetDependencies(idx);
		for (auto &dep : dependencies) {
			auto &dep_dependents = dependencies_map[dep];
			dep_dependents.erase(idx);
			D_ASSERT(!dep_dependents.count(new_idx));
			dep_dependents.insert(new_idx);
		}
	}
	if (has_dependents) {
		D_ASSERT(!dependencies_map.count(new_idx));
		dependencies_map[new_idx] = move(dependencies_map[idx]);
		dependencies_map.erase(idx);
	}
	if (has_dependencies) {
		D_ASSERT(!dependents_map.count(new_idx));
		dependents_map[new_idx] = move(dependents_map[idx]);
		dependents_map.erase(idx);
	}
}

vector<column_t> ColumnDependencyManager::CleanupInternals(column_t column_amount) {
	vector<column_t> to_adjust;
	D_ASSERT(!deleted_columns.empty());
	// Get the lowest index that was deleted
	vector<column_t> new_indices(column_amount, DConstants::INVALID_INDEX);
	column_t threshold = *deleted_columns.begin();

	idx_t offset = 0;
	for (column_t i = 0; i < column_amount; i++) {
		new_indices[i] = i - offset;
		if (deleted_columns.count(i)) {
			offset++;
			continue;
		}
		if (i > threshold && (HasDependencies(i) || HasDependents(i))) {
			to_adjust.push_back(i);
		}
	}

	// Adjust all indices inside the dependency managers internal mappings
	for (auto &col : to_adjust) {
		offset = col - new_indices[col];
		AdjustSingle(col, offset);
	}
	deleted_columns.clear();
	return new_indices;
}

stack<column_t> ColumnDependencyManager::GetBindOrder(const vector<ColumnDefinition> &columns) {
	stack<column_t> bind_order;
	queue<column_t> to_visit;
	unordered_set<column_t> visited;

	for (auto &entry : direct_dependencies) {
		auto dependent = entry.first;
		//! Skip the dependents that are also dependencies
		if (dependencies_map.find(dependent) != dependencies_map.end()) {
			continue;
		}
		bind_order.push(dependent);
		visited.insert(dependent);
		for (auto &dependency : direct_dependencies[dependent]) {
			to_visit.push(dependency);
		}
	}

	while (!to_visit.empty()) {
		auto column = to_visit.front();
		to_visit.pop();

		//! If this column does not have dependencies, the queue stops getting filled
		if (direct_dependencies.find(column) == direct_dependencies.end()) {
			continue;
		}
		bind_order.push(column);
		visited.insert(column);

		for (auto &dependency : direct_dependencies[column]) {
			to_visit.push(dependency);
		}
	}

	// Add generated columns that have no dependencies, but still might need to have their type resolved
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		// Not a generated column
		if (!col.Generated()) {
			continue;
		}
		// Already added to the bind_order stack
		if (visited.count(i)) {
			continue;
		}
		bind_order.push(i);
	}

	return bind_order;
}

} // namespace duckdb



namespace duckdb {

CopyFunctionCatalogEntry::CopyFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                   CreateCopyFunctionInfo *info)
    : StandardEntry(CatalogType::COPY_FUNCTION_ENTRY, schema, catalog, info->name), function(info->function) {
}

} // namespace duckdb



namespace duckdb {

IndexCatalogEntry::IndexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info)
    : StandardEntry(CatalogType::INDEX_ENTRY, schema, catalog, info->index_name), index(nullptr), sql(info->sql) {
}

IndexCatalogEntry::~IndexCatalogEntry() {
	// remove the associated index from the info
	if (!info || !index) {
		return;
	}
	info->indexes.RemoveIndex(index);
}

string IndexCatalogEntry::ToSQL() {
	if (sql.empty()) {
		throw InternalException("Cannot convert INDEX to SQL because it was not created with a SQL statement");
	}
	if (sql[sql.size() - 1] != ';') {
		sql += ";";
	}
	return sql;
}

} // namespace duckdb



namespace duckdb {

PragmaFunctionCatalogEntry::PragmaFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                       CreatePragmaFunctionInfo *info)
    : StandardEntry(CatalogType::PRAGMA_FUNCTION_ENTRY, schema, catalog, info->name), functions(move(info->functions)) {
}

} // namespace duckdb






namespace duckdb {

MacroCatalogEntry::MacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : StandardEntry(
          (info->function->type == MacroType::SCALAR_MACRO ? CatalogType::MACRO_ENTRY : CatalogType::TABLE_MACRO_ENTRY),
          schema, catalog, info->name),
      function(move(info->function)) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

ScalarMacroCatalogEntry::ScalarMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : MacroCatalogEntry(catalog, schema, info) {
}

void ScalarMacroCatalogEntry::Serialize(Serializer &main_serializer) {
	D_ASSERT(!internal);
	auto &scalar_function = (ScalarMacroFunction &)*function;
	FieldWriter writer(main_serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(*scalar_function.expression);
	// writer.WriteSerializableList(function->parameters);
	writer.WriteSerializableList(function->parameters);
	writer.WriteField<uint32_t>((uint32_t)function->default_parameters.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<CreateMacroInfo> ScalarMacroCatalogEntry::Deserialize(Deserializer &main_source) {
	auto info = make_unique<CreateMacroInfo>(CatalogType::MACRO_ENTRY);
	FieldReader reader(main_source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	auto expression = reader.ReadRequiredSerializable<ParsedExpression>();
	auto func = make_unique<ScalarMacroFunction>(move(expression));
	info->function = move(func);
	info->function->parameters = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto default_param_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}
	// dont like this
	// info->type=CatalogType::MACRO_ENTRY;
	reader.Finalize();
	return info;
}

TableMacroCatalogEntry::TableMacroCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateMacroInfo *info)
    : MacroCatalogEntry(catalog, schema, info) {
}

void TableMacroCatalogEntry::Serialize(Serializer &main_serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(main_serializer);

	auto &table_function = (TableMacroFunction &)*function;
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(*table_function.query_node);
	writer.WriteSerializableList(function->parameters);
	writer.WriteField<uint32_t>((uint32_t)function->default_parameters.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : function->default_parameters) {
		serializer.WriteString(kv.first);
		kv.second->Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<CreateMacroInfo> TableMacroCatalogEntry::Deserialize(Deserializer &main_source) {
	auto info = make_unique<CreateMacroInfo>(CatalogType::TABLE_MACRO_ENTRY);
	FieldReader reader(main_source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	auto query_node = reader.ReadRequiredSerializable<QueryNode>();
	auto table_function = make_unique<TableMacroFunction>(move(query_node));
	info->function = move(table_function);
	info->function->parameters = reader.ReadRequiredSerializableList<ParsedExpression>();
	auto default_param_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_param_count; i++) {
		auto name = source.Read<string>();
		info->function->default_parameters[name] = ParsedExpression::Deserialize(source);
	}

	reader.Finalize();

	return info;
}

} // namespace duckdb







































#include <sstream>

namespace duckdb {

void FindForeignKeyInformation(CatalogEntry *entry, AlterForeignKeyType alter_fk_type,
                               vector<unique_ptr<AlterForeignKeyInfo>> &fk_arrays) {
	if (entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto *table_entry = (TableCatalogEntry *)entry;
	for (idx_t i = 0; i < table_entry->constraints.size(); i++) {
		auto &cond = table_entry->constraints[i];
		if (cond->type != ConstraintType::FOREIGN_KEY) {
			continue;
		}
		auto &fk = (ForeignKeyConstraint &)*cond;
		if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
			fk_arrays.push_back(make_unique<AlterForeignKeyInfo>(fk.info.schema, fk.info.table, entry->name,
			                                                     fk.pk_columns, fk.fk_columns, fk.info.pk_keys,
			                                                     fk.info.fk_keys, alter_fk_type));
		} else if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE &&
		           alter_fk_type == AlterForeignKeyType::AFT_DELETE) {
			throw CatalogException("Could not drop the table because this table is main key table of the table \"%s\"",
			                       fk.info.table);
		}
	}
}

SchemaCatalogEntry::SchemaCatalogEntry(Catalog *catalog, string name_p, bool internal)
    : CatalogEntry(CatalogType::SCHEMA_ENTRY, catalog, move(name_p)),
      tables(*catalog, make_unique<DefaultViewGenerator>(*catalog, this)), indexes(*catalog), table_functions(*catalog),
      copy_functions(*catalog), pragma_functions(*catalog),
      functions(*catalog, make_unique<DefaultFunctionGenerator>(*catalog, this)), sequences(*catalog),
      collations(*catalog), types(*catalog, make_unique<DefaultTypeGenerator>(*catalog, this)) {
	this->internal = internal;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict, unordered_set<CatalogEntry *> dependencies) {
	auto entry_name = entry->name;
	auto entry_type = entry->type;
	auto result = entry.get();

	// first find the set for this entry
	auto &set = GetCatalogSet(entry_type);

	if (name != TEMP_SCHEMA) {
		dependencies.insert(this);
	} else {
		entry->temporary = true;
	}
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE: first try to drop the entry
		auto old_entry = set.GetEntry(context, entry_name);
		if (old_entry) {
			if (old_entry->type != entry_type) {
				throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", entry_name,
				                       CatalogTypeToString(old_entry->type), CatalogTypeToString(entry_type));
			}
			(void)set.DropEntry(context, entry_name, false);
		}
	}
	// now try to add the entry
	if (!set.CreateEntry(context, entry_name, move(entry), dependencies)) {
		// entry already exists!
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("%s with name \"%s\" already exists!", CatalogTypeToString(entry_type), entry_name);
		} else {
			return nullptr;
		}
	}
	return result;
}

CatalogEntry *SchemaCatalogEntry::AddEntry(ClientContext &context, unique_ptr<StandardEntry> entry,
                                           OnCreateConflict on_conflict) {
	unordered_set<CatalogEntry *> dependencies;
	return AddEntry(context, move(entry), on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto sequence = make_unique<SequenceCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(sequence), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto type_entry = make_unique<TypeCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(type_entry), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto table = make_unique<TableCatalogEntry>(catalog, this, info);
	table->storage->info->cardinality = table->storage->GetTotalRows();

	CatalogEntry *entry = AddEntry(context, move(table), info->Base().on_conflict, info->dependencies);
	if (!entry) {
		return nullptr;
	}

	// add a foreign key constraint in main key table if there is a foreign key constraint
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(entry, AlterForeignKeyType::AFT_ADD, fk_arrays);
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key table
		AlterForeignKeyInfo *fk_info = fk_arrays[i].get();
		catalog->Alter(context, fk_info);

		// make a dependency between this table and referenced table
		auto &set = GetCatalogSet(CatalogType::TABLE_ENTRY);
		info->dependencies.insert(set.GetEntry(context, fk_info->name));
	}
	return entry;
}

CatalogEntry *SchemaCatalogEntry::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto view = make_unique<ViewCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(view), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) {
	unordered_set<CatalogEntry *> dependencies;
	dependencies.insert(table);
	auto index = make_unique<IndexCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(index), info->on_conflict, dependencies);
}

CatalogEntry *SchemaCatalogEntry::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto collation = make_unique<CollateCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(collation), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto table_function = make_unique<TableFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(table_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto copy_function = make_unique<CopyFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(copy_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto pragma_function = make_unique<PragmaFunctionCatalogEntry>(catalog, this, info);
	return AddEntry(context, move(pragma_function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	unique_ptr<StandardEntry> function;
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		function = make_unique_base<StandardEntry, ScalarFunctionCatalogEntry>(catalog, this,
		                                                                       (CreateScalarFunctionInfo *)info);
		break;
	case CatalogType::MACRO_ENTRY:
		// create a macro function
		function = make_unique_base<StandardEntry, ScalarMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
		break;

	case CatalogType::TABLE_MACRO_ENTRY:
		// create a macro function
		function = make_unique_base<StandardEntry, TableMacroCatalogEntry>(catalog, this, (CreateMacroInfo *)info);
		break;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		D_ASSERT(info->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
		// create an aggregate function
		function = make_unique_base<StandardEntry, AggregateFunctionCatalogEntry>(catalog, this,
		                                                                          (CreateAggregateFunctionInfo *)info);
		break;
	default:
		throw InternalException("Unknown function type \"%s\"", CatalogTypeToString(info->type));
	}
	return AddEntry(context, move(function), info->on_conflict);
}

CatalogEntry *SchemaCatalogEntry::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto entry = GetCatalogSet(info->type).GetEntry(context, info->name);
	if (!entry) {
		return CreateFunction(context, info);
	}

	info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	switch (info->type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY: {
		auto scalar_info = (CreateScalarFunctionInfo *)info;
		auto &scalars = *(ScalarFunctionCatalogEntry *)entry;
		for (const auto &scalar : scalars.functions) {
			scalar_info->functions.emplace_back(scalar);
		}
		break;
	}
	case CatalogType::AGGREGATE_FUNCTION_ENTRY: {
		auto agg_info = (CreateAggregateFunctionInfo *)info;
		auto &aggs = *(AggregateFunctionCatalogEntry *)entry;
		for (const auto &agg : aggs.functions) {
			agg_info->functions.AddFunction(agg);
		}
		break;
	}
	default:
		// Macros can only be replaced because there is only one of each name.
		throw InternalException("Unsupported function type \"%s\" for adding", CatalogTypeToString(info->type));
	}
	return CreateFunction(context, info);
}

void SchemaCatalogEntry::DropEntry(ClientContext &context, DropInfo *info) {
	auto &set = GetCatalogSet(info->type);

	// first find the entry
	auto existing_entry = set.GetEntry(context, info->name);
	if (!existing_entry) {
		if (!info->if_exists) {
			throw CatalogException("%s with name \"%s\" does not exist!", CatalogTypeToString(info->type), info->name);
		}
		return;
	}
	if (existing_entry->type != info->type) {
		throw CatalogException("Existing object %s is of type %s, trying to replace with type %s", info->name,
		                       CatalogTypeToString(existing_entry->type), CatalogTypeToString(info->type));
	}

	// if there is a foreign key constraint, get that information
	vector<unique_ptr<AlterForeignKeyInfo>> fk_arrays;
	FindForeignKeyInformation(existing_entry, AlterForeignKeyType::AFT_DELETE, fk_arrays);

	if (!set.DropEntry(context, info->name, info->cascade)) {
		throw InternalException("Could not drop element because of an internal error");
	}

	// remove the foreign key constraint in main key table if main key table's name is valid
	for (idx_t i = 0; i < fk_arrays.size(); i++) {
		// alter primary key tablee
		Catalog::GetCatalog(context).Alter(context, fk_arrays[i].get());
	}
}

void SchemaCatalogEntry::Alter(ClientContext &context, AlterInfo *info) {
	CatalogType type = info->GetCatalogType();
	auto &set = GetCatalogSet(type);
	if (info->type == AlterType::CHANGE_OWNERSHIP) {
		if (!set.AlterOwnership(context, (ChangeOwnershipInfo *)info)) {
			throw CatalogException("Couldn't change ownership!");
		}
	} else {
		string name = info->name;
		if (!set.AlterEntry(context, name, info)) {
			throw CatalogException("Entry with name \"%s\" does not exist!", name);
		}
	}
}

void SchemaCatalogEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(context, callback);
}

void SchemaCatalogEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	auto &set = GetCatalogSet(type);
	set.Scan(callback);
}

void SchemaCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.Finalize();
}

unique_ptr<CreateSchemaInfo> SchemaCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSchemaInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	reader.Finalize();

	return info;
}

string SchemaCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SCHEMA " << name << ";";
	return ss.str();
}

CatalogSet &SchemaCatalogEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::VIEW_ENTRY:
	case CatalogType::TABLE_ENTRY:
		return tables;
	case CatalogType::INDEX_ENTRY:
		return indexes;
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return table_functions;
	case CatalogType::COPY_FUNCTION_ENTRY:
		return copy_functions;
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return pragma_functions;
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::MACRO_ENTRY:
		return functions;
	case CatalogType::SEQUENCE_ENTRY:
		return sequences;
	case CatalogType::COLLATION_ENTRY:
		return collations;
	case CatalogType::TYPE_ENTRY:
		return types;
	default:
		throw InternalException("Unsupported catalog type in schema");
	}
}

} // namespace duckdb








#include <algorithm>
#include <sstream>

namespace duckdb {

SequenceCatalogEntry::SequenceCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateSequenceInfo *info)
    : StandardEntry(CatalogType::SEQUENCE_ENTRY, schema, catalog, info->name), usage_count(info->usage_count),
      counter(info->start_value), increment(info->increment), start_value(info->start_value),
      min_value(info->min_value), max_value(info->max_value), cycle(info->cycle) {
	this->temporary = info->temporary;
}

void SequenceCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteField<uint64_t>(usage_count);
	writer.WriteField<int64_t>(increment);
	writer.WriteField<int64_t>(min_value);
	writer.WriteField<int64_t>(max_value);
	writer.WriteField<int64_t>(counter);
	writer.WriteField<bool>(cycle);
	writer.Finalize();
}

unique_ptr<CreateSequenceInfo> SequenceCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateSequenceInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	info->usage_count = reader.ReadRequired<uint64_t>();
	info->increment = reader.ReadRequired<int64_t>();
	info->min_value = reader.ReadRequired<int64_t>();
	info->max_value = reader.ReadRequired<int64_t>();
	info->start_value = reader.ReadRequired<int64_t>();
	info->cycle = reader.ReadRequired<bool>();
	reader.Finalize();

	return info;
}

string SequenceCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE SEQUENCE ";
	ss << name;
	ss << " INCREMENT BY " << increment;
	ss << " MINVALUE " << min_value;
	ss << " MAXVALUE " << max_value;
	ss << " START " << counter;
	ss << " " << (cycle ? "CYCLE" : "NO CYCLE") << ";";
	return ss.str();
}
} // namespace duckdb
























#include <sstream>

namespace duckdb {

const string &TableCatalogEntry::GetColumnName(column_t index) {
	return columns[index].Name();
}

column_t TableCatalogEntry::GetColumnIndex(string &column_name, bool if_exists) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		// entry not found: try lower-casing the name
		entry = name_map.find(StringUtil::Lower(column_name));
		if (entry == name_map.end()) {
			if (if_exists) {
				return DConstants::INVALID_INDEX;
			}
			throw BinderException("Table \"%s\" does not have a column with name \"%s\"", name, column_name);
		}
	}
	column_name = GetColumnName(entry->second);
	return entry->second;
}

void AddDataTableIndex(DataTable *storage, vector<ColumnDefinition> &columns, vector<idx_t> &keys,
                       IndexConstraintType constraint_type) {
	// fetch types and create expressions for the index from the columns
	vector<column_t> column_ids;
	vector<unique_ptr<Expression>> unbound_expressions;
	vector<unique_ptr<Expression>> bound_expressions;
	idx_t key_nr = 0;
	for (auto &key : keys) {
		D_ASSERT(key < columns.size());
		auto &column = columns[key];
		if (column.Generated()) {
			throw InvalidInputException("Creating index on generated column is not supported");
		}

		unbound_expressions.push_back(make_unique<BoundColumnRefExpression>(columns[key].Name(), columns[key].Type(),
		                                                                    ColumnBinding(0, column_ids.size())));

		bound_expressions.push_back(make_unique<BoundReferenceExpression>(columns[key].Type(), key_nr++));
		column_ids.push_back(column.StorageOid());
	}
	// create an adaptive radix tree around the expressions
	auto art = make_unique<ART>(column_ids, move(unbound_expressions), constraint_type);
	storage->AddIndex(move(art), bound_expressions);
}

TableCatalogEntry::TableCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateTableInfo *info,
                                     std::shared_ptr<DataTable> inherited_storage)
    : StandardEntry(CatalogType::TABLE_ENTRY, schema, catalog, info->Base().table), storage(move(inherited_storage)),
      columns(move(info->Base().columns)), constraints(move(info->Base().constraints)),
      bound_constraints(move(info->bound_constraints)),
      column_dependency_manager(move(info->column_dependency_manager)) {
	this->temporary = info->Base().temporary;
	// add lower case aliases
	this->name_map = move(info->name_map);
#ifdef DEBUG
	D_ASSERT(name_map.size() == columns.size());
	for (idx_t i = 0; i < columns.size(); i++) {
		D_ASSERT(name_map[columns[i].Name()] == i);
	}
#endif
	// add the "rowid" alias, if there is no rowid column specified in the table
	if (name_map.find("rowid") == name_map.end()) {
		name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
	}
	if (!storage) {
		// create the physical storage
		vector<ColumnDefinition> storage_columns;
		vector<ColumnDefinition> get_columns;
		for (auto &col_def : columns) {
			get_columns.push_back(col_def.Copy());
			if (col_def.Generated()) {
				continue;
			}
			storage_columns.push_back(col_def.Copy());
		}
		storage = make_shared<DataTable>(catalog->db, schema->name, name, move(storage_columns), move(info->data));

		// create the unique indexes for the UNIQUE and PRIMARY KEY and FOREIGN KEY constraints
		for (idx_t i = 0; i < bound_constraints.size(); i++) {
			auto &constraint = bound_constraints[i];
			if (constraint->type == ConstraintType::UNIQUE) {
				// unique constraint: create a unique index
				auto &unique = (BoundUniqueConstraint &)*constraint;
				IndexConstraintType constraint_type = IndexConstraintType::UNIQUE;
				if (unique.is_primary_key) {
					constraint_type = IndexConstraintType::PRIMARY;
				}
				AddDataTableIndex(storage.get(), get_columns, unique.keys, constraint_type);
			} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
				// foreign key constraint: create a foreign key index
				auto &bfk = (BoundForeignKeyConstraint &)*constraint;
				if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
				    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
					AddDataTableIndex(storage.get(), get_columns, bfk.info.fk_keys, IndexConstraintType::FOREIGN);
				}
			}
		}
	}
}

bool TableCatalogEntry::ColumnExists(const string &name) {
	auto iterator = name_map.find(name);
	if (iterator == name_map.end()) {
		return false;
	}
	return true;
}

idx_t TableCatalogEntry::StandardColumnCount() const {
	idx_t count = 0;
	for (auto &col : columns) {
		if (col.Category() == TableColumnType::STANDARD) {
			count++;
		}
	}
	return count;
}

unique_ptr<CatalogEntry> TableCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	D_ASSERT(!internal);
	if (info->type != AlterType::ALTER_TABLE) {
		throw CatalogException("Can only modify table with ALTER TABLE statement");
	}
	auto table_info = (AlterTableInfo *)info;
	switch (table_info->alter_table_type) {
	case AlterTableType::RENAME_COLUMN: {
		auto rename_info = (RenameColumnInfo *)table_info;
		return RenameColumn(context, *rename_info);
	}
	case AlterTableType::RENAME_TABLE: {
		auto rename_info = (RenameTableInfo *)table_info;
		auto copied_table = Copy(context);
		copied_table->name = rename_info->new_table_name;
		return copied_table;
	}
	case AlterTableType::ADD_COLUMN: {
		auto add_info = (AddColumnInfo *)table_info;
		return AddColumn(context, *add_info);
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto remove_info = (RemoveColumnInfo *)table_info;
		return RemoveColumn(context, *remove_info);
	}
	case AlterTableType::SET_DEFAULT: {
		auto set_default_info = (SetDefaultInfo *)table_info;
		return SetDefault(context, *set_default_info);
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto change_type_info = (ChangeColumnTypeInfo *)table_info;
		return ChangeColumnType(context, *change_type_info);
	}
	case AlterTableType::FOREIGN_KEY_CONSTRAINT: {
		auto foreign_key_constraint_info = (AlterForeignKeyInfo *)table_info;
		return SetForeignKeyConstraint(context, *foreign_key_constraint_info);
	}
	default:
		throw InternalException("Unrecognized alter table type!");
	}
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.column_names.back() == info.old_name) {
			colref.column_names.back() = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

unique_ptr<CatalogEntry> TableCatalogEntry::RenameColumn(ClientContext &context, RenameColumnInfo &info) {
	auto rename_idx = GetColumnIndex(info.old_name);
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();

		if (rename_idx == i) {
			copy.SetName(info.new_name);
		}
		create_info->columns.push_back(move(copy));
		auto &col = create_info->columns[i];
		if (col.Generated() && column_dependency_manager.IsDependencyOf(i, rename_idx)) {
			RenameExpression(col.GeneratedExpressionMutable(), info);
		}
	}
	for (idx_t c_idx = 0; c_idx < constraints.size(); c_idx++) {
		auto copy = constraints[c_idx]->Copy();
		switch (copy->type) {
		case ConstraintType::NOT_NULL:
			// NOT NULL constraint: no adjustments necessary
			break;
		case ConstraintType::CHECK: {
			// CHECK constraint: need to rename column references that refer to the renamed column
			auto &check = (CheckConstraint &)*copy;
			RenameExpression(*check.expression, info);
			break;
		}
		case ConstraintType::UNIQUE: {
			// UNIQUE constraint: possibly need to rename columns
			auto &unique = (UniqueConstraint &)*copy;
			for (idx_t i = 0; i < unique.columns.size(); i++) {
				if (unique.columns[i] == info.old_name) {
					unique.columns[i] = info.new_name;
				}
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			// FOREIGN KEY constraint: possibly need to rename columns
			auto &fk = (ForeignKeyConstraint &)*copy;
			vector<string> columns = fk.pk_columns;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				columns = fk.fk_columns;
			} else if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < fk.fk_columns.size(); i++) {
					columns.push_back(fk.fk_columns[i]);
				}
			}
			for (idx_t i = 0; i < columns.size(); i++) {
				if (columns[i] == info.old_name) {
					throw CatalogException(
					    "Cannot rename column \"%s\" because this is involved in the foreign key constraint",
					    info.old_name);
				}
			}
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(copy));
	}
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::AddColumn(ClientContext &context, AddColumnInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}
	for (auto &constraint : constraints) {
		create_info->constraints.push_back(constraint->Copy());
	}
	Binder::BindLogicalType(context, info.new_column.TypeMutable(), schema->name);
	info.new_column.SetOid(columns.size());
	info.new_column.SetStorageOid(storage->column_definitions.size());

	auto col = info.new_column.Copy();

	create_info->columns.push_back(move(col));

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	auto new_storage =
	    make_shared<DataTable>(context, *storage, info.new_column, bound_create_info->bound_defaults.back().get());
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::RemoveColumn(ClientContext &context, RemoveColumnInfo &info) {
	auto removed_index = GetColumnIndex(info.removed_column, info.if_exists);
	if (removed_index == DConstants::INVALID_INDEX) {
		return nullptr;
	}

	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	unordered_set<column_t> removed_columns;
	if (column_dependency_manager.HasDependents(removed_index)) {
		removed_columns = column_dependency_manager.GetDependents(removed_index);
	}
	if (!removed_columns.empty() && !info.cascade) {
		throw CatalogException("Cannot drop column: column is a dependency of 1 or more generated column(s)");
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (i == removed_index || removed_columns.count(i)) {
			continue;
		}
		create_info->columns.push_back(col.Copy());
	}
	if (create_info->columns.empty()) {
		throw CatalogException("Cannot drop column: table only has one column remaining!");
	}
	vector<column_t> adjusted_indices = column_dependency_manager.RemoveColumn(removed_index, columns.size());
	// handle constraints for the new table
	D_ASSERT(constraints.size() == bound_constraints.size());
	for (idx_t constr_idx = 0; constr_idx < constraints.size(); constr_idx++) {
		auto &constraint = constraints[constr_idx];
		auto &bound_constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null_constraint = (BoundNotNullConstraint &)*bound_constraint;
			if (not_null_constraint.index != removed_index) {
				// the constraint is not about this column: we need to copy it
				// we might need to shift the index back by one though, to account for the removed column
				idx_t new_index = not_null_constraint.index;
				new_index = adjusted_indices[new_index];
				create_info->constraints.push_back(make_unique<NotNullConstraint>(new_index));
			}
			break;
		}
		case ConstraintType::CHECK: {
			// CHECK constraint
			auto &bound_check = (BoundCheckConstraint &)*bound_constraint;
			// check if the removed column is part of the check constraint
			if (bound_check.bound_columns.find(removed_index) != bound_check.bound_columns.end()) {
				if (bound_check.bound_columns.size() > 1) {
					// CHECK constraint that concerns mult
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a CHECK constraint that depends on it",
					    info.removed_column);
				} else {
					// CHECK constraint that ONLY concerns this column, strip the constraint
				}
			} else {
				// check constraint does not concern the removed column: simply re-add it
				create_info->constraints.push_back(constraint->Copy());
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto copy = constraint->Copy();
			auto &unique = (UniqueConstraint &)*copy;
			if (unique.index != DConstants::INVALID_INDEX) {
				if (unique.index == removed_index) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a UNIQUE constraint that depends on it",
					    info.removed_column);
				}
				unique.index = adjusted_indices[unique.index];
			}
			create_info->constraints.push_back(move(copy));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto copy = constraint->Copy();
			auto &fk = (ForeignKeyConstraint &)*copy;
			vector<string> columns = fk.pk_columns;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				columns = fk.fk_columns;
			} else if (fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < fk.fk_columns.size(); i++) {
					columns.push_back(fk.fk_columns[i]);
				}
			}
			for (idx_t i = 0; i < columns.size(); i++) {
				if (columns[i] == info.removed_column) {
					throw CatalogException(
					    "Cannot drop column \"%s\" because there is a FOREIGN KEY constraint that depends on it",
					    info.removed_column);
				}
			}
			create_info->constraints.push_back(move(copy));
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	if (columns[removed_index].Generated()) {
		return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
		                                      storage);
	}
	auto new_storage = make_shared<DataTable>(context, *storage, removed_index);
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(),
	                                      new_storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetDefault(ClientContext &context, SetDefaultInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	auto default_idx = GetColumnIndex(info.column_name);

	// Copy all the columns, changing the value of the one that was specified by 'column_name'
	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (default_idx == i) {
			// set the default value of this column
			D_ASSERT(!copy.Generated()); // Shouldnt reach here - DEFAULT value isn't supported for Generated Columns
			copy.SetDefaultValue(info.expression ? info.expression->Copy() : nullptr);
		}
		create_info->columns.push_back(move(copy));
	}
	// Copy all the constraints
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

unique_ptr<CatalogEntry> TableCatalogEntry::ChangeColumnType(ClientContext &context, ChangeColumnTypeInfo &info) {
	if (info.target_type.id() == LogicalTypeId::USER) {
		auto &catalog = Catalog::GetCatalog(context);
		info.target_type = catalog.GetType(context, schema->name, UserType::GetTypeName(info.target_type));
	}
	auto change_idx = GetColumnIndex(info.column_name);
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	for (idx_t i = 0; i < columns.size(); i++) {
		auto copy = columns[i].Copy();
		if (change_idx == i) {
			// set the type of this column
			if (copy.Generated()) {
				throw NotImplementedException("Changing types of generated columns is not supported yet");
				// copy.ChangeGeneratedExpressionType(info.target_type);
			}
			copy.SetType(info.target_type);
		}
		// TODO: check if the generated_expression breaks, only delete it if it does
		if (copy.Generated() && column_dependency_manager.IsDependencyOf(i, change_idx)) {
			throw BinderException(
			    "This column is referenced by the generated column \"%s\", so its type can not be changed",
			    copy.Name());
		}
		create_info->columns.push_back(move(copy));
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		switch (constraint->type) {
		case ConstraintType::CHECK: {
			auto &bound_check = (BoundCheckConstraint &)*bound_constraints[i];
			if (bound_check.bound_columns.find(change_idx) != bound_check.bound_columns.end()) {
				throw BinderException("Cannot change the type of a column that has a CHECK constraint specified");
			}
			break;
		}
		case ConstraintType::NOT_NULL:
			break;
		case ConstraintType::UNIQUE: {
			auto &bound_unique = (BoundUniqueConstraint &)*bound_constraints[i];
			if (bound_unique.key_set.find(change_idx) != bound_unique.key_set.end()) {
				throw BinderException(
				    "Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = (BoundForeignKeyConstraint &)*bound_constraints[i];
			unordered_set<idx_t> key_set = bfk.pk_key_set;
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				key_set = bfk.fk_key_set;
			} else if (bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				for (idx_t i = 0; i < bfk.info.fk_keys.size(); i++) {
					key_set.insert(bfk.info.fk_keys[i]);
				}
			}
			if (key_set.find(change_idx) != key_set.end()) {
				throw BinderException("Cannot change the type of a column that has a FOREIGN KEY constraint specified");
			}
			break;
		}
		default:
			throw InternalException("Unsupported constraint for entry!");
		}
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	// bind the specified expression
	vector<column_t> bound_columns;
	AlterBinder expr_binder(*binder, context, *this, bound_columns, info.target_type);
	auto expression = info.expression->Copy();
	auto bound_expression = expr_binder.Bind(expression);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	if (bound_columns.empty()) {
		bound_columns.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	auto new_storage =
	    make_shared<DataTable>(context, *storage, change_idx, info.target_type, move(bound_columns), *bound_expression);
	auto result =
	    make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), new_storage);
	return move(result);
}

unique_ptr<CatalogEntry> TableCatalogEntry::SetForeignKeyConstraint(ClientContext &context, AlterForeignKeyInfo &info) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	create_info->temporary = temporary;

	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}
	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		if (constraint->type == ConstraintType::FOREIGN_KEY) {
			ForeignKeyConstraint &fk = (ForeignKeyConstraint &)*constraint;
			if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && fk.info.table == info.fk_table) {
				continue;
			}
		}
		create_info->constraints.push_back(move(constraint));
	}
	if (info.type == AlterForeignKeyType::AFT_ADD) {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE;
		fk_info.schema = info.schema;
		fk_info.table = info.fk_table;
		fk_info.pk_keys = info.pk_keys;
		fk_info.fk_keys = info.fk_keys;
		create_info->constraints.push_back(
		    make_unique<ForeignKeyConstraint>(info.pk_columns, info.fk_columns, move(fk_info)));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));

	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

ColumnDefinition &TableCatalogEntry::GetColumn(const string &name) {
	auto entry = name_map.find(name);
	if (entry == name_map.end() || entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		throw CatalogException("Column with name %s does not exist!", name);
	}
	auto column_index = entry->second;
	return columns[column_index];
}

vector<LogicalType> TableCatalogEntry::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : columns) {
		if (it.Generated()) {
			continue;
		}
		types.push_back(it.Type());
	}
	return types;
}

void TableCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);

	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteRegularSerializableList(columns);
	writer.WriteSerializableList(constraints);
	writer.Finalize();
}

unique_ptr<CreateTableInfo> TableCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTableInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->table = reader.ReadRequired<string>();
	info->columns = reader.ReadRequiredSerializableList<ColumnDefinition, ColumnDefinition>();
	info->constraints = reader.ReadRequiredSerializableList<Constraint>();
	reader.Finalize();

	return info;
}

string TableCatalogEntry::ToSQL() {
	std::stringstream ss;

	ss << "CREATE TABLE ";

	if (schema->name != DEFAULT_SCHEMA) {
		ss << KeywordHelper::WriteOptionallyQuoted(schema->name) << ".";
	}

	ss << KeywordHelper::WriteOptionallyQuoted(name) << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key columns
	unordered_set<idx_t> not_null_columns;
	unordered_set<idx_t> unique_columns;
	unordered_set<idx_t> pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = (NotNullConstraint &)*constraint;
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = (UniqueConstraint &)*constraint;
			vector<string> constraint_columns = pk.columns;
			if (pk.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				extra_constraints.push_back(constraint->ToString());
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = (ForeignKeyConstraint &)*constraint;
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			ss << ", ";
		}
		auto &column = columns[i];
		ss << KeywordHelper::WriteOptionallyQuoted(column.Name()) << " ";
		ss << column.Type().ToString();
		bool not_null = not_null_columns.find(column.Oid()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Oid()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Oid()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.DefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue()->ToString() << ")";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ");";
	return ss.str();
}

unique_ptr<CatalogEntry> TableCatalogEntry::Copy(ClientContext &context) {
	auto create_info = make_unique<CreateTableInfo>(schema->name, name);
	for (idx_t i = 0; i < columns.size(); i++) {
		create_info->columns.push_back(columns[i].Copy());
	}

	for (idx_t i = 0; i < constraints.size(); i++) {
		auto constraint = constraints[i]->Copy();
		create_info->constraints.push_back(move(constraint));
	}

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(create_info));
	return make_unique<TableCatalogEntry>(catalog, schema, (BoundCreateTableInfo *)bound_create_info.get(), storage);
}

void TableCatalogEntry::SetAsRoot() {
	storage->SetAsRoot();
}

void TableCatalogEntry::CommitAlter(AlterInfo &info) {
	D_ASSERT(info.type == AlterType::ALTER_TABLE);
	auto &alter_table = (AlterTableInfo &)info;
	string column_name;
	switch (alter_table.alter_table_type) {
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove_info = (RemoveColumnInfo &)alter_table;
		column_name = remove_info.removed_column;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change_info = (ChangeColumnTypeInfo &)alter_table;
		column_name = change_info.column_name;
		break;
	}
	default:
		break;
	}
	if (column_name.empty()) {
		return;
	}
	idx_t removed_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < columns.size(); i++) {
		auto &col = columns[i];
		if (col.Name() == column_name) {
			// No need to alter storage, removed column is generated column
			if (col.Generated()) {
				return;
			}
			removed_index = i;
			break;
		}
	}
	D_ASSERT(removed_index != DConstants::INVALID_INDEX);
	storage->CommitDropColumn(removed_index);
}

void TableCatalogEntry::CommitDrop() {
	storage->CommitDropTable();
}

} // namespace duckdb



namespace duckdb {

TableFunctionCatalogEntry::TableFunctionCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema,
                                                     CreateTableFunctionInfo *info)
    : StandardEntry(CatalogType::TABLE_FUNCTION_ENTRY, schema, catalog, info->name), functions(move(info->functions)) {
	D_ASSERT(this->functions.size() > 0);
}

} // namespace duckdb









#include <algorithm>
#include <sstream>

namespace duckdb {

TypeCatalogEntry::TypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTypeInfo *info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info->name), user_type(info->type) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

void TypeCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(user_type);
	writer.Finalize();
}

unique_ptr<CreateTypeInfo> TypeCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTypeInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	info->type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	reader.Finalize();

	return info;
}

string TypeCatalogEntry::ToSQL() {
	std::stringstream ss;
	switch (user_type.id()) {
	case (LogicalTypeId::ENUM): {
		Vector values_insert_order(EnumType::GetValuesInsertOrder(user_type));
		idx_t size = EnumType::GetSize(user_type);
		ss << "CREATE TYPE ";
		ss << KeywordHelper::WriteOptionallyQuoted(name);
		ss << " AS ENUM ( ";

		for (idx_t i = 0; i < size; i++) {
			ss << "'" << values_insert_order.GetValue(i).ToString() << "'";
			if (i != size - 1) {
				ss << ", ";
			}
		}
		ss << ");";
		break;
	}
	default:
		throw InternalException("Logical Type can't be used as a User Defined Type");
	}

	return ss.str();
}

} // namespace duckdb









#include <algorithm>

namespace duckdb {

void ViewCatalogEntry::Initialize(CreateViewInfo *info) {
	query = move(info->query);
	this->aliases = info->aliases;
	this->types = info->types;
	this->temporary = info->temporary;
	this->sql = info->sql;
	this->internal = info->internal;
}

ViewCatalogEntry::ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInfo *info)
    : StandardEntry(CatalogType::VIEW_ENTRY, schema, catalog, info->view_name) {
	Initialize(info);
}

unique_ptr<CatalogEntry> ViewCatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	D_ASSERT(!internal);
	if (info->type != AlterType::ALTER_VIEW) {
		throw CatalogException("Can only modify view with ALTER VIEW statement");
	}
	auto view_info = (AlterViewInfo *)info;
	switch (view_info->alter_view_type) {
	case AlterViewType::RENAME_VIEW: {
		auto rename_info = (RenameViewInfo *)view_info;
		auto copied_view = Copy(context);
		copied_view->name = rename_info->new_view_name;
		return copied_view;
	}
	default:
		throw InternalException("Unrecognized alter view type!");
	}
}

void ViewCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteString(sql);
	writer.WriteSerializable(*query);
	writer.WriteList<string>(aliases);
	writer.WriteRegularSerializableList<LogicalType>(types);
	writer.Finalize();
}

unique_ptr<CreateViewInfo> ViewCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateViewInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->view_name = reader.ReadRequired<string>();
	info->sql = reader.ReadRequired<string>();
	info->query = reader.ReadRequiredSerializable<SelectStatement>();
	info->aliases = reader.ReadRequiredList<string>();
	info->types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	reader.Finalize();

	return info;
}

string ViewCatalogEntry::ToSQL() {
	if (sql.empty()) {
		//! Return empty sql with view name so pragma view_tables don't complain
		return sql;
	}
	return sql + "\n;";
}

unique_ptr<CatalogEntry> ViewCatalogEntry::Copy(ClientContext &context) {
	D_ASSERT(!internal);
	auto create_info = make_unique<CreateViewInfo>(schema->name, name);
	create_info->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	for (idx_t i = 0; i < aliases.size(); i++) {
		create_info->aliases.push_back(aliases[i]);
	}
	for (idx_t i = 0; i < types.size(); i++) {
		create_info->types.push_back(types[i]);
	}
	create_info->temporary = temporary;
	create_info->sql = sql;

	return make_unique<ViewCatalogEntry>(catalog, schema, create_info.get());
}

} // namespace duckdb




namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p)
    : oid(catalog_p->ModifyCatalog()), type(type), catalog(catalog_p), set(nullptr), name(move(name_p)), deleted(false),
      temporary(false), internal(false), parent(nullptr) {
}

CatalogEntry::~CatalogEntry() {
}

void CatalogEntry::SetAsRoot() {
}

// LCOV_EXCL_START
unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	throw InternalException("Unsupported alter type for catalog entry!");
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) {
	throw InternalException("Unsupported copy type for catalog entry!");
}

string CatalogEntry::ToSQL() {
	throw InternalException("Unsupported catalog type for ToSQL()");
}
// LCOV_EXCL_STOP

} // namespace duckdb








namespace duckdb {

CatalogSearchPath::CatalogSearchPath(ClientContext &context_p) : context(context_p) {
	SetPaths(ParsePaths(""));
}

void CatalogSearchPath::Set(const string &new_value, bool is_set_schema) {
	auto new_paths = ParsePaths(new_value);
	if (is_set_schema && new_paths.size() != 1) {
		throw CatalogException("SET schema can set only 1 schema. This has %d", new_paths.size());
	}
	auto &catalog = Catalog::GetCatalog(context);
	for (const auto &path : new_paths) {
		if (!catalog.GetSchema(context, StringUtil::Lower(path), true)) {
			throw CatalogException("SET %s: No schema named %s found.", is_set_schema ? "schema" : "search_path", path);
		}
	}
	this->set_paths = move(new_paths);
	SetPaths(set_paths);
}

const vector<string> &CatalogSearchPath::Get() {
	return paths;
}

const string &CatalogSearchPath::GetOrDefault(const string &name) {
	return name == INVALID_SCHEMA ? GetDefault() : name; // NOLINT
}

const string &CatalogSearchPath::GetDefault() {
	const auto &paths = Get();
	D_ASSERT(paths.size() >= 2);
	D_ASSERT(paths[0] == TEMP_SCHEMA);
	return paths[1];
}

void CatalogSearchPath::SetPaths(vector<string> new_paths) {
	paths.clear();
	paths.reserve(new_paths.size() + 3);
	paths.emplace_back(TEMP_SCHEMA);
	for (auto &path : new_paths) {
		paths.push_back(move(path));
	}
	paths.emplace_back(DEFAULT_SCHEMA);
	paths.emplace_back("pg_catalog");
}

vector<string> CatalogSearchPath::ParsePaths(const string &value) {
	return StringUtil::SplitWithQuote(StringUtil::Lower(value));
}

} // namespace duckdb













namespace duckdb {

//! Class responsible to keep track of state when removing entries from the catalog.
//! When deleting, many types of errors can be thrown, since we want to avoid try/catch blocks
//! this class makes sure that whatever elements were modified are returned to a correct state
//! when exceptions are thrown.
//! The idea here is to use RAII (Resource acquisition is initialization) to mimic a try/catch/finally block.
//! If any exception is raised when this object exists, then its destructor will be called
//! and the entry will return to its previous state during deconstruction.
class EntryDropper {
public:
	//! Both constructor and destructor are privates because they should only be called by DropEntryDependencies
	explicit EntryDropper(CatalogSet &catalog_set, idx_t entry_index)
	    : catalog_set(catalog_set), entry_index(entry_index) {
		old_deleted = catalog_set.entries[entry_index].get()->deleted;
	}

	~EntryDropper() {
		catalog_set.entries[entry_index].get()->deleted = old_deleted;
	}

private:
	//! The current catalog_set
	CatalogSet &catalog_set;
	//! Keeps track of the state of the entry before starting the delete
	bool old_deleted;
	//! Index of entry to be deleted
	idx_t entry_index;
};

CatalogSet::CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults)
    : catalog(catalog), defaults(move(defaults)) {
}

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
                             unordered_set<CatalogEntry *> &dependencies) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// lock this catalog set to disallow reading
	unique_lock<mutex> read_lock(catalog_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// if it does not: entry has never been created

		// check if there is a default entry
		auto entry = CreateDefaultEntry(context, name, read_lock);
		if (entry) {
			return false;
		}

		// first create a dummy deleted entry for this entry
		// so transactions started before the commit of this transaction don't
		// see it yet
		entry_index = current_entry++;
		auto dummy_node = make_unique<CatalogEntry>(CatalogType::INVALID, value->catalog, name);
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries[entry_index] = move(dummy_node);
		PutMapping(context, name, entry_index);
	} else {
		entry_index = mapping_value->index;
		auto &current = *entries[entry_index];
		// if it does, we have to check version numbers
		if (HasConflict(context, current.timestamp)) {
			// current version has been written to by a currently active
			// transaction
			throw TransactionException("Catalog write-write conflict on create with \"%s\"", current.name);
		}
		// there is a current version that has been committed
		// if it has not been deleted there is a conflict
		if (!current.deleted) {
			return false;
		}
	}
	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	value->timestamp = transaction.transaction_id;
	value->set = this;

	// now add the dependency set of this object to the dependency manager
	catalog.dependency_manager->AddObject(context, value.get(), dependencies);

	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());
	entries[entry_index] = move(value);
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&catalog_entry) {
	catalog_entry = entries[entry_index].get();
	// if it does: we have to retrieve the entry and to check version numbers
	if (HasConflict(context, catalog_entry->timestamp)) {
		// current version has been written to by a currently active
		// transaction
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", catalog_entry->name);
	}
	// there is a current version that has been committed by this transaction
	if (catalog_entry->deleted) {
		// if the entry was already deleted, it now does not exist anymore
		// so we return that we could not find it
		return false;
	}
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index,
                                  CatalogEntry *&catalog_entry) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// the entry does not exist, check if we can create a default entry
		return false;
	}
	entry_index = mapping_value->index;
	return GetEntryInternal(context, entry_index, catalog_entry);
}

bool CatalogSet::AlterOwnership(ClientContext &context, ChangeOwnershipInfo *info) {
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, info->name, entry_index, entry)) {
		return false;
	}

	auto owner_entry = catalog.GetEntry(context, info->owner_schema, info->owner_name);
	if (!owner_entry) {
		return false;
	}

	catalog.dependency_manager->AddOwnership(context, owner_entry, entry);

	return true;
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	auto &transaction = Transaction::GetTransaction(context);
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// first check if the entry exists in the unordered set
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot alter entry \"%s\" because it is an internal system entry", entry->name);
	}

	// lock this catalog set to disallow reading
	lock_guard<mutex> read_lock(catalog_lock);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it to the updated table node
	string original_name = entry->name;
	auto value = entry->AlterEntry(context, alter_info);
	if (!value) {
		// alter failed, but did not result in an error
		return true;
	}

	if (value->name != original_name) {
		auto mapping_value = GetMapping(context, value->name);
		if (mapping_value && !mapping_value->deleted) {
			auto entry = GetEntryForTransaction(context, entries[mapping_value->index].get());
			if (!entry->deleted) {
				string rename_err_msg =
				    "Could not rename \"%s\" to \"%s\": another entry with this name already exists!";
				throw CatalogException(rename_err_msg, original_name, value->name);
			}
		}
		PutMapping(context, value->name, entry_index);
		DeleteMapping(context, original_name);
	}
	//! Check the dependency manager to verify that there are no conflicting dependencies with this alter
	catalog.dependency_manager->AlterObject(context, entry, value.get());

	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;

	// serialize the AlterInfo into a temporary buffer
	BufferedSerializer serializer;
	alter_info->Serialize(serializer);
	BinaryData serialized_alter = serializer.GetData();

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get(), serialized_alter.data.get(), serialized_alter.size);
	entries[entry_index] = move(value);

	return true;
}

void CatalogSet::DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {

	// Stores the deleted value of the entry before starting the process
	EntryDropper dropper(*this, entry_index);

	// To correctly delete the object and its dependencies, it temporarily is set to deleted.
	entries[entry_index].get()->deleted = true;

	// check any dependencies of this object
	entry.catalog->dependency_manager->DropObject(context, &entry, cascade);

	// dropper destructor is called here
	// the destructor makes sure to return the value to the previous state
	// dropper.~EntryDropper()
}

void CatalogSet::DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	auto &transaction = Transaction::GetTransaction(context);

	DropEntryDependencies(context, entry_index, entry, cascade);

	// create a new entry and replace the currently stored one
	// set the timestamp to the timestamp of the current transaction
	// and point it at the dummy node
	auto value = make_unique<CatalogEntry>(CatalogType::DELETED_ENTRY, entry.catalog, entry.name);
	value->timestamp = transaction.transaction_id;
	value->child = move(entries[entry_index]);
	value->child->parent = value.get();
	value->set = this;
	value->deleted = true;

	// push the old entry in the undo buffer for this transaction
	transaction.PushCatalogEntry(value->child.get());

	entries[entry_index] = move(value);
}

bool CatalogSet::DropEntry(ClientContext &context, const string &name, bool cascade) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);
	// we can only delete an entry that exists
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", entry->name);
	}

	DropEntryInternal(context, entry_index, *entry, cascade);
	return true;
}

void CatalogSet::CleanupEntry(CatalogEntry *catalog_entry) {
	// destroy the backed up entry: it is no longer required
	D_ASSERT(catalog_entry->parent);
	if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
		lock_guard<mutex> lock(catalog_lock);
		if (!catalog_entry->deleted) {
			// delete the entry from the dependency manager, if it is not deleted yet
			catalog_entry->catalog->dependency_manager->EraseObject(catalog_entry);
		}
		auto parent = catalog_entry->parent;
		parent->child = move(catalog_entry->child);
		if (parent->deleted && !parent->child && !parent->parent) {
			auto mapping_entry = mapping.find(parent->name);
			D_ASSERT(mapping_entry != mapping.end());
			auto index = mapping_entry->second->index;
			auto entry = entries.find(index);
			D_ASSERT(entry != entries.end());
			if (entry->second.get() == parent) {
				mapping.erase(mapping_entry);
				entries.erase(entry);
			}
		}
	}
}

bool CatalogSet::HasConflict(ClientContext &context, transaction_t timestamp) {
	auto &transaction = Transaction::GetTransaction(context);
	return (timestamp >= TRANSACTION_ID_START && timestamp != transaction.transaction_id) ||
	       (timestamp < TRANSACTION_ID_START && timestamp > transaction.start_time);
}

MappingValue *CatalogSet::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;
	auto entry = mapping.find(name);
	if (entry != mapping.end()) {
		mapping_value = entry->second.get();
	} else {

		return nullptr;
	}
	if (get_latest) {
		return mapping_value;
	}
	while (mapping_value->child) {
		if (UseTimestamp(context, mapping_value->timestamp)) {
			break;
		}
		mapping_value = mapping_value->child.get();
		D_ASSERT(mapping_value);
	}
	return mapping_value;
}

void CatalogSet::PutMapping(ClientContext &context, const string &name, idx_t entry_index) {
	auto entry = mapping.find(name);
	auto new_value = make_unique<MappingValue>(entry_index);
	new_value->timestamp = Transaction::GetTransaction(context).transaction_id;
	if (entry != mapping.end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value.get();
	}
	mapping[name] = move(new_value);
}

void CatalogSet::DeleteMapping(ClientContext &context, const string &name) {
	auto entry = mapping.find(name);
	D_ASSERT(entry != mapping.end());
	auto delete_marker = make_unique<MappingValue>(entry->second->index);
	delete_marker->deleted = true;
	delete_marker->timestamp = Transaction::GetTransaction(context).transaction_id;
	delete_marker->child = move(entry->second);
	delete_marker->child->parent = delete_marker.get();
	mapping[name] = move(delete_marker);
}

bool CatalogSet::UseTimestamp(ClientContext &context, transaction_t timestamp) {
	auto &transaction = Transaction::GetTransaction(context);
	if (timestamp == transaction.transaction_id) {
		// we created this version
		return true;
	}
	if (timestamp < transaction.start_time) {
		// this version was commited before we started the transaction
		return true;
	}
	return false;
}

CatalogEntry *CatalogSet::GetEntryForTransaction(ClientContext &context, CatalogEntry *current) {
	while (current->child) {
		if (UseTimestamp(context, current->timestamp)) {
			break;
		}
		current = current->child.get();
		D_ASSERT(current);
	}
	return current;
}

CatalogEntry *CatalogSet::GetCommittedEntry(CatalogEntry *current) {
	while (current->child) {
		if (current->timestamp < TRANSACTION_ID_START) {
			// this entry is committed: use it
			break;
		}
		current = current->child.get();
		D_ASSERT(current);
	}
	return current;
}

pair<string, idx_t> CatalogSet::SimilarEntry(ClientContext &context, const string &name) {
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(context, lock);

	string result;
	idx_t current_score = (idx_t)-1;
	for (auto &kv : mapping) {
		auto mapping_value = GetMapping(context, kv.first);
		if (mapping_value && !mapping_value->deleted) {
			auto ldist = StringUtil::LevenshteinDistance(kv.first, name);
			if (ldist < current_score) {
				current_score = ldist;
				result = kv.first;
			}
		}
	}
	return {result, current_score};
}

CatalogEntry *CatalogSet::CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	if (mapping.find(entry->name) != mapping.end()) {
		return nullptr;
	}
	auto &name = entry->name;
	auto entry_index = current_entry++;
	auto catalog_entry = entry.get();

	entry->set = this;
	entry->timestamp = 0;

	PutMapping(context, name, entry_index);
	mapping[name]->timestamp = 0;
	entries[entry_index] = move(entry);
	return catalog_entry;
}

CatalogEntry *CatalogSet::CreateDefaultEntry(ClientContext &context, const string &name, unique_lock<mutex> &lock) {
	// no entry found with this name, check for defaults
	if (!defaults || defaults->created_all_entries) {
		// no defaults either: return null
		return nullptr;
	}
	// this catalog set has a default map defined
	// check if there is a default entry that we can create with this name
	lock.unlock();
	auto entry = defaults->CreateDefaultEntry(context, name);

	lock.lock();
	if (!entry) {
		// no default entry
		return nullptr;
	}
	// there is a default entry! create it
	auto result = CreateEntryInternal(context, move(entry));
	if (result) {
		return result;
	}
	// we found a default entry, but failed
	// this means somebody else created the entry first
	// just retry?
	lock.unlock();
	return GetEntry(context, name);
}

CatalogEntry *CatalogSet::GetEntry(ClientContext &context, const string &name) {
	unique_lock<mutex> lock(catalog_lock);
	auto mapping_value = GetMapping(context, name);
	if (mapping_value != nullptr && !mapping_value->deleted) {
		// we found an entry for this name
		// check the version numbers

		auto catalog_entry = entries[mapping_value->index].get();
		CatalogEntry *current = GetEntryForTransaction(context, catalog_entry);
		if (current->deleted || (current->name != name && !UseTimestamp(context, mapping_value->timestamp))) {
			return nullptr;
		}
		return current;
	}
	return CreateDefaultEntry(context, name, lock);
}

void CatalogSet::UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp) {
	entry->timestamp = timestamp;
	mapping[entry->name]->timestamp = timestamp;
}

void CatalogSet::AdjustUserDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove) {
	CatalogEntry *user_type_catalog = (CatalogEntry *)LogicalType::GetCatalog(column.Type());
	if (user_type_catalog) {
		if (remove) {
			catalog.dependency_manager->dependents_map[user_type_catalog].erase(entry->parent);
			catalog.dependency_manager->dependencies_map[entry->parent].erase(user_type_catalog);
		} else {
			catalog.dependency_manager->dependents_map[user_type_catalog].insert(entry);
			catalog.dependency_manager->dependencies_map[entry].insert(user_type_catalog);
		}
	}
}

void CatalogSet::AdjustDependency(CatalogEntry *entry, TableCatalogEntry *table, ColumnDefinition &column,
                                  bool remove) {
	bool found = false;
	if (column.Type().id() == LogicalTypeId::ENUM) {
		for (auto &old_column : table->columns) {
			if (old_column.Name() == column.Name() && old_column.Type().id() != LogicalTypeId::ENUM) {
				AdjustUserDependency(entry, column, remove);
				found = true;
			}
		}
		if (!found) {
			AdjustUserDependency(entry, column, remove);
		}
	} else if (!(column.Type().GetAlias().empty())) {
		auto alias = column.Type().GetAlias();
		for (auto &old_column : table->columns) {
			auto old_alias = old_column.Type().GetAlias();
			if (old_column.Name() == column.Name() && old_alias != alias) {
				AdjustUserDependency(entry, column, remove);
				found = true;
			}
		}
		if (!found) {
			AdjustUserDependency(entry, column, remove);
		}
	}
}

void CatalogSet::AdjustTableDependencies(CatalogEntry *entry) {
	if (entry->type == CatalogType::TABLE_ENTRY && entry->parent->type == CatalogType::TABLE_ENTRY) {
		// If it's a table entry we have to check for possibly removing or adding user type dependencies
		auto old_table = (TableCatalogEntry *)entry->parent;
		auto new_table = (TableCatalogEntry *)entry;

		for (auto &new_column : new_table->columns) {
			AdjustDependency(entry, old_table, new_column, false);
		}
		for (auto &old_column : old_table->columns) {
			AdjustDependency(entry, new_table, old_column, true);
		}
	}
}

void CatalogSet::Undo(CatalogEntry *entry) {
	lock_guard<mutex> write_lock(catalog.write_lock);

	lock_guard<mutex> lock(catalog_lock);

	// entry has to be restored
	// and entry->parent has to be removed ("rolled back")

	// i.e. we have to place (entry) as (entry->parent) again
	auto &to_be_removed_node = entry->parent;

	AdjustTableDependencies(entry);

	if (!to_be_removed_node->deleted) {
		// delete the entry from the dependency manager as well
		catalog.dependency_manager->EraseObject(to_be_removed_node);
	}
	if (entry->name != to_be_removed_node->name) {
		// rename: clean up the new name when the rename is rolled back
		auto removed_entry = mapping.find(to_be_removed_node->name);
		if (removed_entry->second->child) {
			removed_entry->second->child->parent = nullptr;
			mapping[to_be_removed_node->name] = move(removed_entry->second->child);
		} else {
			mapping.erase(removed_entry);
		}
	}
	if (to_be_removed_node->parent) {
		// if the to be removed node has a parent, set the child pointer to the
		// to be restored node
		to_be_removed_node->parent->child = move(to_be_removed_node->child);
		entry->parent = to_be_removed_node->parent;
	} else {
		// otherwise we need to update the base entry tables
		auto &name = entry->name;
		to_be_removed_node->child->SetAsRoot();
		entries[mapping[name]->index] = move(to_be_removed_node->child);
		entry->parent = nullptr;
	}

	// restore the name if it was deleted
	auto restored_entry = mapping.find(entry->name);
	if (restored_entry->second->deleted || entry->type == CatalogType::INVALID) {
		if (restored_entry->second->child) {
			restored_entry->second->child->parent = nullptr;
			mapping[entry->name] = move(restored_entry->second->child);
		} else {
			mapping.erase(restored_entry);
		}
	}
	// we mark the catalog as being modified, since this action can lead to e.g. tables being dropped
	entry->catalog->ModifyCatalog();
}

void CatalogSet::CreateDefaultEntries(ClientContext &context, unique_lock<mutex> &lock) {
	if (!defaults || defaults->created_all_entries) {
		return;
	}
	// this catalog set has a default set defined:
	auto default_entries = defaults->GetDefaultEntries();
	for (auto &default_entry : default_entries) {
		auto map_entry = mapping.find(default_entry);
		if (map_entry == mapping.end()) {
			// we unlock during the CreateEntry, since it might reference other catalog sets...
			// specifically for views this can happen since the view will be bound
			lock.unlock();
			auto entry = defaults->CreateDefaultEntry(context, default_entry);
			if (!entry) {
				throw InternalException("Failed to create default entry for %s", default_entry);
			}

			lock.lock();
			CreateEntryInternal(context, move(entry));
		}
	}
	defaults->created_all_entries = true;
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	unique_lock<mutex> lock(catalog_lock);
	CreateDefaultEntries(context, lock);

	for (auto &kv : entries) {
		auto entry = kv.second.get();
		entry = GetEntryForTransaction(context, entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}

void CatalogSet::Scan(const std::function<void(CatalogEntry *)> &callback) {
	// lock the catalog set
	lock_guard<mutex> lock(catalog_lock);
	for (auto &kv : entries) {
		auto entry = kv.second.get();
		entry = GetCommittedEntry(entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}
} // namespace duckdb









namespace duckdb {

static DefaultMacro internal_macros[] = {
	{DEFAULT_SCHEMA, "current_user", {nullptr}, "'duckdb'"},                       // user name of current execution context
	{DEFAULT_SCHEMA, "current_catalog", {nullptr}, "'duckdb'"},                    // name of current database (called "catalog" in the SQL standard)
	{DEFAULT_SCHEMA, "current_database", {nullptr}, "'duckdb'"},                   // name of current database
	{DEFAULT_SCHEMA, "user", {nullptr}, "current_user"},                           // equivalent to current_user
	{DEFAULT_SCHEMA, "session_user", {nullptr}, "'duckdb'"},                       // session user name
	{"pg_catalog", "inet_client_addr", {nullptr}, "NULL"},                       // address of the remote connection
	{"pg_catalog", "inet_client_port", {nullptr}, "NULL"},                       // port of the remote connection
	{"pg_catalog", "inet_server_addr", {nullptr}, "NULL"},                       // address of the local connection
	{"pg_catalog", "inet_server_port", {nullptr}, "NULL"},                       // port of the local connection
	{"pg_catalog", "pg_my_temp_schema", {nullptr}, "0"},                         // OID of session's temporary schema, or 0 if none
	{"pg_catalog", "pg_is_other_temp_schema", {"schema_id", nullptr}, "false"},  // is schema another session's temporary schema?

	{"pg_catalog", "pg_conf_load_time", {nullptr}, "current_timestamp"},         // configuration load time
	{"pg_catalog", "pg_postmaster_start_time", {nullptr}, "current_timestamp"},  // server start time

	{"pg_catalog", "pg_typeof", {"expression", nullptr}, "lower(typeof(expression))"},  // get the data type of any value

	// privilege functions
	// {"has_any_column_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for any column of table
	{"pg_catalog", "has_any_column_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for any column of table
	// {"has_column_privilege", {"user", "table", "column", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for column
	{"pg_catalog", "has_column_privilege", {"table", "column", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for column
	// {"has_database_privilege", {"user", "database", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for database
	{"pg_catalog", "has_database_privilege", {"database", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for database
	// {"has_foreign_data_wrapper_privilege", {"user", "fdw", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign-data wrapper
	{"pg_catalog", "has_foreign_data_wrapper_privilege", {"fdw", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign-data wrapper
	// {"has_function_privilege", {"user", "function", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for function
	{"pg_catalog", "has_function_privilege", {"function", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for function
	// {"has_language_privilege", {"user", "language", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for language
	{"pg_catalog", "has_language_privilege", {"language", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for language
	// {"has_schema_privilege", {"user", "schema, privilege", nullptr}, "true"},  //boolean  //does user have privilege for schema
	{"pg_catalog", "has_schema_privilege", {"schema", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for schema
	// {"has_sequence_privilege", {"user", "sequence", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for sequence
	{"pg_catalog", "has_sequence_privilege", {"sequence", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for sequence
	// {"has_server_privilege", {"user", "server", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for foreign server
	{"pg_catalog", "has_server_privilege", {"server", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for foreign server
	// {"has_table_privilege", {"user", "table", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for table
	{"pg_catalog", "has_table_privilege", {"table", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for table
	// {"has_tablespace_privilege", {"user", "tablespace", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for tablespace
	{"pg_catalog", "has_tablespace_privilege", {"tablespace", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for tablespace

	// various postgres system functions
	{"pg_catalog", "pg_get_viewdef", {"oid", nullptr}, "(select sql from duckdb_views() v where v.view_oid=oid)"},
	{"pg_catalog", "pg_get_constraintdef", {"constraint_oid", "pretty_bool", nullptr}, "(select constraint_text from duckdb_constraints() d_constraint where d_constraint.table_oid=constraint_oid/1000000 and d_constraint.constraint_index=constraint_oid%1000000)"},
	{"pg_catalog", "pg_get_expr", {"pg_node_tree", "relation_oid", nullptr}, "pg_node_tree"},
	{"pg_catalog", "format_pg_type", {"type_name", nullptr}, "case when logical_type='FLOAT' then 'real' when logical_type='DOUBLE' then 'double precision' when logical_type='DECIMAL' then 'numeric' when logical_type='VARCHAR' then 'character varying' when logical_type='BLOB' then 'bytea' when logical_type='TIMESTAMP' then 'timestamp without time zone' when logical_type='TIME' then 'time without time zone' else lower(logical_type) end"},
	{"pg_catalog", "format_type", {"type_oid", "typemod", nullptr}, "(select format_pg_type(type_name) from duckdb_types() t where t.type_oid=type_oid) || case when typemod>0 then concat('(', typemod/1000, ',', typemod%1000, ')') else '' end"},

	{"pg_catalog", "pg_has_role", {"user", "role", "privilege", nullptr}, "true"},  //boolean  //does user have privilege for role
	{"pg_catalog", "pg_has_role", {"role", "privilege", nullptr}, "true"},  //boolean  //does current user have privilege for role

	{"pg_catalog", "col_description", {"table_oid", "column_number", nullptr}, "NULL"},   // get comment for a table column
	{"pg_catalog", "obj_description", {"object_oid", "catalog_name", nullptr}, "NULL"},   // get comment for a database object
	{"pg_catalog", "shobj_description", {"object_oid", "catalog_name", nullptr}, "NULL"}, // get comment for a shared database object

	// visibility functions
	{"pg_catalog", "pg_collation_is_visible", {"collation_oid", nullptr}, "true"},
	{"pg_catalog", "pg_conversion_is_visible", {"conversion_oid", nullptr}, "true"},
	{"pg_catalog", "pg_function_is_visible", {"function_oid", nullptr}, "true"},
	{"pg_catalog", "pg_opclass_is_visible", {"opclass_oid", nullptr}, "true"},
	{"pg_catalog", "pg_operator_is_visible", {"operator_oid", nullptr}, "true"},
	{"pg_catalog", "pg_opfamily_is_visible", {"opclass_oid", nullptr}, "true"},
	{"pg_catalog", "pg_table_is_visible", {"table_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_config_is_visible", {"config_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_dict_is_visible", {"dict_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_parser_is_visible", {"parser_oid", nullptr}, "true"},
	{"pg_catalog", "pg_ts_template_is_visible", {"template_oid", nullptr}, "true"},
	{"pg_catalog", "pg_type_is_visible", {"type_oid", nullptr}, "true"},

	{DEFAULT_SCHEMA, "round_even", {"x", "n", nullptr}, "CASE ((abs(x) * power(10, n+1)) % 10) WHEN 5 THEN round(x/2, n) * 2 ELSE round(x, n) END"},
	{DEFAULT_SCHEMA, "roundbankers", {"x", "n", nullptr}, "round_even(x, n)"},
	{DEFAULT_SCHEMA, "nullif", {"a", "b", nullptr}, "CASE WHEN a=b THEN NULL ELSE a END"},
	{DEFAULT_SCHEMA, "list_append", {"l", "e", nullptr}, "list_concat(l, list_value(e))"},
	{DEFAULT_SCHEMA, "array_append", {"arr", "el", nullptr}, "list_append(arr, el)"},
	{DEFAULT_SCHEMA, "list_prepend", {"e", "l", nullptr}, "list_concat(list_value(e), l)"},
	{DEFAULT_SCHEMA, "array_prepend", {"el", "arr", nullptr}, "list_prepend(el, arr)"},
	{DEFAULT_SCHEMA, "array_pop_back", {"arr", nullptr}, "arr[:LEN(arr)-1]"},
	{DEFAULT_SCHEMA, "array_pop_front", {"arr", nullptr}, "arr[2:]"},
	{DEFAULT_SCHEMA, "array_push_back", {"arr", "e", nullptr}, "list_concat(arr, list_value(e))"},
	{DEFAULT_SCHEMA, "array_push_front", {"arr", "e", nullptr}, "list_concat(list_value(e), arr)"},
	{DEFAULT_SCHEMA, "generate_subscripts", {"arr", "dim", nullptr}, "unnest(generate_series(1, array_length(arr, dim)))"},
	{DEFAULT_SCHEMA, "fdiv", {"x", "y", nullptr}, "floor(x/y)"},
	{DEFAULT_SCHEMA, "fmod", {"x", "y", nullptr}, "(x-y*floor(x/y))"},

	// algebraic list aggregates
	{DEFAULT_SCHEMA, "list_avg", {"l", nullptr}, "list_aggr(l, 'avg')"},
	{DEFAULT_SCHEMA, "list_var_samp", {"l", nullptr}, "list_aggr(l, 'var_samp')"},
	{DEFAULT_SCHEMA, "list_var_pop", {"l", nullptr}, "list_aggr(l, 'var_pop')"},
	{DEFAULT_SCHEMA, "list_stddev_pop", {"l", nullptr}, "list_aggr(l, 'stddev_pop')"},
	{DEFAULT_SCHEMA, "list_stddev_samp", {"l", nullptr}, "list_aggr(l, 'stddev_samp')"},
	{DEFAULT_SCHEMA, "list_sem", {"l", nullptr}, "list_aggr(l, 'sem')"},

	// distributive list aggregates
	{DEFAULT_SCHEMA, "list_approx_count_distinct", {"l", nullptr}, "list_aggr(l, 'approx_count_distinct')"},
	{DEFAULT_SCHEMA, "list_bit_xor", {"l", nullptr}, "list_aggr(l, 'bit_xor')"},
	{DEFAULT_SCHEMA, "list_bit_or", {"l", nullptr}, "list_aggr(l, 'bit_or')"},
	{DEFAULT_SCHEMA, "list_bit_and", {"l", nullptr}, "list_aggr(l, 'bit_and')"},
	{DEFAULT_SCHEMA, "list_bool_and", {"l", nullptr}, "list_aggr(l, 'bool_and')"},
	{DEFAULT_SCHEMA, "list_bool_or", {"l", nullptr}, "list_aggr(l, 'bool_or')"},
	{DEFAULT_SCHEMA, "list_count", {"l", nullptr}, "list_aggr(l, 'count')"},
	{DEFAULT_SCHEMA, "list_entropy", {"l", nullptr}, "list_aggr(l, 'entropy')"},
	{DEFAULT_SCHEMA, "list_last", {"l", nullptr}, "list_aggr(l, 'last')"},
	{DEFAULT_SCHEMA, "list_first", {"l", nullptr}, "list_aggr(l, 'first')"},
	{DEFAULT_SCHEMA, "list_kurtosis", {"l", nullptr}, "list_aggr(l, 'kurtosis')"},
	{DEFAULT_SCHEMA, "list_min", {"l", nullptr}, "list_aggr(l, 'min')"},
	{DEFAULT_SCHEMA, "list_max", {"l", nullptr}, "list_aggr(l, 'max')"},
	{DEFAULT_SCHEMA, "list_product", {"l", nullptr}, "list_aggr(l, 'product')"},
	{DEFAULT_SCHEMA, "list_skewness", {"l", nullptr}, "list_aggr(l, 'skewness')"},
	{DEFAULT_SCHEMA, "list_sum", {"l", nullptr}, "list_aggr(l, 'sum')"},
	{DEFAULT_SCHEMA, "list_string_agg", {"l", nullptr}, "list_aggr(l, 'string_agg')"},

	// holistic list aggregates
	{DEFAULT_SCHEMA, "list_mode", {"l", nullptr}, "list_aggr(l, 'mode')"},
	{DEFAULT_SCHEMA, "list_median", {"l", nullptr}, "list_aggr(l, 'median')"},
	{DEFAULT_SCHEMA, "list_mad", {"l", nullptr}, "list_aggr(l, 'mad')"},

	// nested list aggregates
	{DEFAULT_SCHEMA, "list_histogram", {"l", nullptr}, "list_aggr(l, 'histogram')"},

	{nullptr, nullptr, {nullptr}, nullptr}
	};

unique_ptr<CreateMacroInfo> DefaultFunctionGenerator::CreateInternalTableMacroInfo(DefaultMacro &default_macro, unique_ptr<MacroFunction> function) {
	for (idx_t param_idx = 0; default_macro.parameters[param_idx] != nullptr; param_idx++) {
		function->parameters.push_back(
		    make_unique<ColumnRefExpression>(default_macro.parameters[param_idx]));
	}

	auto bind_info = make_unique<CreateMacroInfo>();
	bind_info->schema = default_macro.schema;
	bind_info->name = default_macro.name;
	bind_info->temporary = true;
	bind_info->internal = true;
	bind_info->type = function->type == MacroType::TABLE_MACRO ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY;
	bind_info->function = move(function);
	return bind_info;

}

unique_ptr<CreateMacroInfo> DefaultFunctionGenerator::CreateInternalMacroInfo(DefaultMacro &default_macro) {
	// parse the expression
	auto expressions = Parser::ParseExpressionList(default_macro.macro);
	D_ASSERT(expressions.size() == 1);

	auto result = make_unique<ScalarMacroFunction>(move(expressions[0]));
	return CreateInternalTableMacroInfo(default_macro, move(result));
}

unique_ptr<CreateMacroInfo> DefaultFunctionGenerator::CreateInternalTableMacroInfo(DefaultMacro &default_macro) {
	Parser parser;
	parser.ParseQuery(default_macro.macro);
	D_ASSERT(parser.statements.size() == 1);
	D_ASSERT(parser.statements[0]->type == StatementType::SELECT_STATEMENT);

	auto &select = (SelectStatement &) *parser.statements[0];
	auto result = make_unique<TableMacroFunction>(move(select.node));
	return CreateInternalTableMacroInfo(default_macro, move(result));
}

static unique_ptr<CreateFunctionInfo> GetDefaultFunction(const string &input_schema, const string &input_name) {
	auto schema = StringUtil::Lower(input_schema);
	auto name = StringUtil::Lower(input_name);
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].schema == schema && internal_macros[index].name == name) {
			return DefaultFunctionGenerator::CreateInternalMacroInfo(internal_macros[index]);
		}
	}
	return nullptr;
}

DefaultFunctionGenerator::DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultFunctionGenerator::CreateDefaultEntry(ClientContext &context,
                                                                      const string &entry_name) {
	auto info = GetDefaultFunction(schema->name, entry_name);
	if (info) {
		return make_unique_base<CatalogEntry, ScalarMacroCatalogEntry>(&catalog, schema, (CreateMacroInfo *)info.get());
	}
	return nullptr;
}

vector<string> DefaultFunctionGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].schema == schema->name) {
			result.emplace_back(internal_macros[index].name);
		}
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

struct DefaultSchema {
	const char *name;
};

static DefaultSchema internal_schemas[] = {{"information_schema"}, {"pg_catalog"}, {nullptr}};

static bool GetDefaultSchema(const string &input_schema) {
	auto schema = StringUtil::Lower(input_schema);
	for (idx_t index = 0; internal_schemas[index].name != nullptr; index++) {
		if (internal_schemas[index].name == schema) {
			return true;
		}
	}
	return false;
}

DefaultSchemaGenerator::DefaultSchemaGenerator(Catalog &catalog) : DefaultGenerator(catalog) {
}

unique_ptr<CatalogEntry> DefaultSchemaGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	if (GetDefaultSchema(entry_name)) {
		return make_unique_base<CatalogEntry, SchemaCatalogEntry>(&catalog, StringUtil::Lower(entry_name), true);
	}
	return nullptr;
}

vector<string> DefaultSchemaGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_schemas[index].name != nullptr; index++) {
		result.emplace_back(internal_schemas[index].name);
	}
	return result;
}

} // namespace duckdb







namespace duckdb {

struct DefaultType {
	const char *name;
	LogicalTypeId type;
};

static DefaultType internal_types[] = {{"int", LogicalTypeId::INTEGER},
                                       {"int4", LogicalTypeId::INTEGER},
                                       {"signed", LogicalTypeId::INTEGER},
                                       {"integer", LogicalTypeId::INTEGER},
                                       {"integral", LogicalTypeId::INTEGER},
                                       {"int32", LogicalTypeId::INTEGER},
                                       {"varchar", LogicalTypeId::VARCHAR},
                                       {"bpchar", LogicalTypeId::VARCHAR},
                                       {"text", LogicalTypeId::VARCHAR},
                                       {"string", LogicalTypeId::VARCHAR},
                                       {"char", LogicalTypeId::VARCHAR},
                                       {"nvarchar", LogicalTypeId::VARCHAR},
                                       {"bytea", LogicalTypeId::BLOB},
                                       {"blob", LogicalTypeId::BLOB},
                                       {"varbinary", LogicalTypeId::BLOB},
                                       {"binary", LogicalTypeId::BLOB},
                                       {"int8", LogicalTypeId::BIGINT},
                                       {"bigint", LogicalTypeId::BIGINT},
                                       {"int64", LogicalTypeId::BIGINT},
                                       {"long", LogicalTypeId::BIGINT},
                                       {"oid", LogicalTypeId::BIGINT},
                                       {"int2", LogicalTypeId::SMALLINT},
                                       {"smallint", LogicalTypeId::SMALLINT},
                                       {"short", LogicalTypeId::SMALLINT},
                                       {"int16", LogicalTypeId::SMALLINT},
                                       {"timestamp", LogicalTypeId::TIMESTAMP},
                                       {"datetime", LogicalTypeId::TIMESTAMP},
                                       {"timestamp_us", LogicalTypeId::TIMESTAMP},
                                       {"timestamp_ms", LogicalTypeId::TIMESTAMP_MS},
                                       {"timestamp_ns", LogicalTypeId::TIMESTAMP_NS},
                                       {"timestamp_s", LogicalTypeId::TIMESTAMP_SEC},
                                       {"bool", LogicalTypeId::BOOLEAN},
                                       {"boolean", LogicalTypeId::BOOLEAN},
                                       {"logical", LogicalTypeId::BOOLEAN},
                                       {"decimal", LogicalTypeId::DECIMAL},
                                       {"dec", LogicalTypeId::DECIMAL},
                                       {"numeric", LogicalTypeId::DECIMAL},
                                       {"real", LogicalTypeId::FLOAT},
                                       {"float4", LogicalTypeId::FLOAT},
                                       {"float", LogicalTypeId::FLOAT},
                                       {"double", LogicalTypeId::DOUBLE},
                                       {"float8", LogicalTypeId::DOUBLE},
                                       {"tinyint", LogicalTypeId::TINYINT},
                                       {"int1", LogicalTypeId::TINYINT},
                                       {"date", LogicalTypeId::DATE},
                                       {"time", LogicalTypeId::TIME},
                                       {"interval", LogicalTypeId::INTERVAL},
                                       {"hugeint", LogicalTypeId::HUGEINT},
                                       {"int128", LogicalTypeId::HUGEINT},
                                       {"uuid", LogicalTypeId::UUID},
                                       {"guid", LogicalTypeId::UUID},
                                       {"struct", LogicalTypeId::STRUCT},
                                       {"row", LogicalTypeId::STRUCT},
                                       {"list", LogicalTypeId::LIST},
                                       {"map", LogicalTypeId::MAP},
                                       {"utinyint", LogicalTypeId::UTINYINT},
                                       {"uint8", LogicalTypeId::UTINYINT},
                                       {"usmallint", LogicalTypeId::USMALLINT},
                                       {"uint16", LogicalTypeId::USMALLINT},
                                       {"uinteger", LogicalTypeId::UINTEGER},
                                       {"uint32", LogicalTypeId::UINTEGER},
                                       {"ubigint", LogicalTypeId::UBIGINT},
                                       {"uint64", LogicalTypeId::UBIGINT},
                                       {"timestamptz", LogicalTypeId::TIMESTAMP_TZ},
                                       {"timetz", LogicalTypeId::TIME_TZ},
                                       {"json", LogicalTypeId::JSON},
                                       {"null", LogicalTypeId::SQLNULL},
                                       {nullptr, LogicalTypeId::INVALID}};

LogicalTypeId DefaultTypeGenerator::GetDefaultType(const string &name) {
	auto lower_str = StringUtil::Lower(name);
	for (idx_t index = 0; internal_types[index].name != nullptr; index++) {
		if (internal_types[index].name == lower_str) {
			return internal_types[index].type;
		}
	}
	return LogicalTypeId::INVALID;
}

DefaultTypeGenerator::DefaultTypeGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultTypeGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	if (schema->name != DEFAULT_SCHEMA) {
		return nullptr;
	}
	auto type_id = GetDefaultType(entry_name);
	if (type_id == LogicalTypeId::INVALID) {
		return nullptr;
	}
	CreateTypeInfo info;
	info.name = entry_name;
	info.type = LogicalType(type_id);
	info.internal = true;
	info.temporary = true;
	return make_unique_base<CatalogEntry, TypeCatalogEntry>(&catalog, schema, &info);
}

vector<string> DefaultTypeGenerator::GetDefaultEntries() {
	vector<string> result;
	if (schema->name != DEFAULT_SCHEMA) {
		return result;
	}
	for (idx_t index = 0; internal_types[index].name != nullptr; index++) {
		result.emplace_back(internal_types[index].name);
	}
	return result;
}

} // namespace duckdb








namespace duckdb {

struct DefaultView {
	const char *schema;
	const char *name;
	const char *sql;
};

static DefaultView internal_views[] = {
    {DEFAULT_SCHEMA, "pragma_database_list", "SELECT * FROM pragma_database_list()"},
    {DEFAULT_SCHEMA, "sqlite_master", "select 'table' \"type\", table_name \"name\", table_name \"tbl_name\", 0 rootpage, sql from duckdb_tables union all select 'view' \"type\", view_name \"name\", view_name \"tbl_name\", 0 rootpage, sql from duckdb_views union all select 'index' \"type\", index_name \"name\", table_name \"tbl_name\", 0 rootpage, sql from duckdb_indexes;"},
    {DEFAULT_SCHEMA, "sqlite_schema", "SELECT * FROM sqlite_master"},
    {DEFAULT_SCHEMA, "sqlite_temp_master", "SELECT * FROM sqlite_master"},
    {DEFAULT_SCHEMA, "sqlite_temp_schema", "SELECT * FROM sqlite_master"},
    {DEFAULT_SCHEMA, "duckdb_constraints", "SELECT * FROM duckdb_constraints()"},
    {DEFAULT_SCHEMA, "duckdb_columns", "SELECT * FROM duckdb_columns() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "duckdb_indexes", "SELECT * FROM duckdb_indexes()"},
    {DEFAULT_SCHEMA, "duckdb_schemas", "SELECT * FROM duckdb_schemas() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "duckdb_tables", "SELECT * FROM duckdb_tables() WHERE NOT internal"},
    {DEFAULT_SCHEMA, "duckdb_types", "SELECT * FROM duckdb_types()"},
    {DEFAULT_SCHEMA, "duckdb_views", "SELECT * FROM duckdb_views() WHERE NOT internal"},
    {"pg_catalog", "pg_am", "SELECT 0 oid, 'art' amname, NULL amhandler, 'i' amtype"},
    {"pg_catalog", "pg_attribute", "SELECT table_oid attrelid, column_name attname, data_type_id atttypid, 0 attstattarget, NULL attlen, column_index attnum, 0 attndims, -1 attcacheoff, case when data_type ilike '%decimal%' then numeric_precision*1000+numeric_scale else -1 end atttypmod, false attbyval, NULL attstorage, NULL attalign, NOT is_nullable attnotnull, column_default IS NOT NULL atthasdef, false atthasmissing, '' attidentity, '' attgenerated, false attisdropped, true attislocal, 0 attinhcount, 0 attcollation, NULL attcompression, NULL attacl, NULL attoptions, NULL attfdwoptions, NULL attmissingval FROM duckdb_columns()"},
    {"pg_catalog", "pg_attrdef", "SELECT column_index oid, table_oid adrelid, column_index adnum, column_default adbin from duckdb_columns() where column_default is not null;"},
    {"pg_catalog", "pg_class", "SELECT table_oid oid, table_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, estimated_size::real reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, index_count > 0 relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'r' relkind, column_count relnatts, check_constraint_count relchecks, false relhasoids, has_primary_key relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_tables() UNION ALL SELECT view_oid oid, view_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, 0 reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, false relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'v' relkind, column_count relnatts, 0 relchecks, false relhasoids, false relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_views() UNION ALL SELECT sequence_oid oid, sequence_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, 0 reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, false relhasindex, false relisshared, case when temporary then 't' else 'p' end relpersistence, 'S' relkind, 0 relnatts, 0 relchecks, false relhasoids, false relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_sequences() UNION ALL SELECT index_oid oid, index_name relname, schema_oid relnamespace, 0 reltype, 0 reloftype, 0 relowner, 0 relam, 0 relfilenode, 0 reltablespace, 0 relpages, 0 reltuples, 0 relallvisible, 0 reltoastrelid, 0 reltoastidxid, false relhasindex, false relisshared, 't' relpersistence, 'i' relkind, NULL relnatts, 0 relchecks, false relhasoids, false relhaspkey, false relhasrules, false relhastriggers, false relhassubclass, false relrowsecurity, true relispopulated, NULL relreplident, false relispartition, 0 relrewrite, 0 relfrozenxid, NULL relminmxid, NULL relacl, NULL reloptions, NULL relpartbound FROM duckdb_indexes()"},
    {"pg_catalog", "pg_constraint", "SELECT table_oid*1000000+constraint_index oid, constraint_text conname, schema_oid connamespace, CASE WHEN constraint_type='CHECK' then 'c' WHEN constraint_type='UNIQUE' then 'u' WHEN constraint_type='PRIMARY KEY' THEN 'p' ELSE 'x' END contype, false condeferrable, false condeferred, true convalidated, table_oid conrelid, 0 contypid, 0 conindid, 0 conparentid, 0 confrelid, NULL confupdtype, NULL confdeltype, NULL confmatchtype, true conislocal, 0 coninhcount, false connoinherit, constraint_column_indexes conkey, NULL confkey, NULL conpfeqop, NULL conppeqop, NULL conffeqop, NULL conexclop, expression conbin FROM duckdb_constraints()"},
    {"pg_catalog", "pg_depend", "SELECT * FROM duckdb_dependencies()"},
	{"pg_catalog", "pg_description", "SELECT NULL objoid, NULL classoid, NULL objsubid, NULL description WHERE 1=0"},
    {"pg_catalog", "pg_enum", "SELECT NULL oid, NULL enumtypid, NULL enumsortorder, NULL enumlabel WHERE 1=0"},
    {"pg_catalog", "pg_index", "SELECT index_oid indexrelid, table_oid indrelid, 0 indnatts, 0 indnkeyatts, is_unique indisunique, is_primary indisprimary, false indisexclusion, true indimmediate, false indisclustered, true indisvalid, false indcheckxmin, true indisready, true indislive, false indisreplident, NULL::INT[] indkey, NULL::OID[] indcollation, NULL::OID[] indclass, NULL::INT[] indoption, expressions indexprs, NULL indpred FROM duckdb_indexes()"},
    {"pg_catalog", "pg_indexes", "SELECT schema_name schemaname, table_name tablename, index_name indexname, NULL \"tablespace\", sql indexdef FROM duckdb_indexes()"},
    {"pg_catalog", "pg_namespace", "SELECT oid, schema_name nspname, 0 nspowner, NULL nspacl FROM duckdb_schemas()"},
    {"pg_catalog", "pg_sequence", "SELECT sequence_oid seqrelid, 0 seqtypid, start_value seqstart, increment_by seqincrement, max_value seqmax, min_value seqmin, 0 seqcache, cycle seqcycle FROM duckdb_sequences()"},
	{"pg_catalog", "pg_sequences", "SELECT schema_name schemaname, sequence_name sequencename, 'duckdb' sequenceowner, 0 data_type, start_value, min_value, max_value, increment_by, cycle, 0 cache_size, last_value FROM duckdb_sequences()"},
    {"pg_catalog", "pg_tables", "SELECT schema_name schemaname, table_name tablename, 'duckdb' tableowner, NULL \"tablespace\", index_count > 0 hasindexes, false hasrules, false hastriggers FROM duckdb_tables()"},
    {"pg_catalog", "pg_tablespace", "SELECT 0 oid, 'pg_default' spcname, 0 spcowner, NULL spcacl, NULL spcoptions"},
    {"pg_catalog", "pg_type", "SELECT type_oid oid, format_pg_type(type_name) typname, schema_oid typnamespace, 0 typowner, type_size typlen, false typbyval, 'b' typtype, CASE WHEN type_category='NUMERIC' THEN 'N' WHEN type_category='STRING' THEN 'S' WHEN type_category='DATETIME' THEN 'D' WHEN type_category='BOOLEAN' THEN 'B' WHEN type_category='COMPOSITE' THEN 'C' WHEN type_category='USER' THEN 'U' ELSE 'X' END typcategory, false typispreferred, true typisdefined, NULL typdelim, NULL typrelid, NULL typsubscript, NULL typelem, NULL typarray, NULL typinput, NULL typoutput, NULL typreceive, NULL typsend, NULL typmodin, NULL typmodout, NULL typanalyze, 'd' typalign, 'p' typstorage, NULL typnotnull, NULL typbasetype, NULL typtypmod, NULL typndims, NULL typcollation, NULL typdefaultbin, NULL typdefault, NULL typacl FROM duckdb_types();"},
    {"pg_catalog", "pg_views", "SELECT schema_name schemaname, view_name viewname, 'duckdb' viewowner, sql definition FROM duckdb_views()"},
    {"information_schema", "columns", "SELECT NULL table_catalog, schema_name table_schema, table_name, column_name, column_index ordinal_position, column_default, CASE WHEN is_nullable THEN 'YES' ELSE 'NO' END is_nullable, data_type, character_maximum_length, NULL character_octet_length, numeric_precision, numeric_precision_radix, numeric_scale, NULL datetime_precision, NULL interval_type, NULL interval_precision, NULL character_set_catalog, NULL character_set_schema, NULL character_set_name, NULL collation_catalog, NULL collation_schema, NULL collation_name, NULL domain_catalog, NULL domain_schema, NULL domain_name, NULL udt_catalog, NULL udt_schema, NULL udt_name, NULL scope_catalog, NULL scope_schema, NULL scope_name, NULL maximum_cardinality, NULL dtd_identifier, NULL is_self_referencing, NULL is_identity, NULL identity_generation, NULL identity_start, NULL identity_increment, NULL identity_maximum, NULL identity_minimum, NULL identity_cycle, NULL is_generated, NULL generation_expression, NULL is_updatable FROM duckdb_columns;"},
    {"information_schema", "schemata", "SELECT NULL catalog_name, schema_name, 'duckdb' schema_owner, NULL default_character_set_catalog, NULL default_character_set_schema, NULL default_character_set_name, sql sql_path FROM duckdb_schemas()"},
    {"information_schema", "tables", "SELECT NULL table_catalog, schema_name table_schema, table_name, CASE WHEN temporary THEN 'LOCAL TEMPORARY' ELSE 'BASE TABLE' END table_type, NULL self_referencing_column_name, NULL reference_generation, NULL user_defined_type_catalog, NULL user_defined_type_schema, NULL user_defined_type_name, 'YES' is_insertable_into, 'NO' is_typed, CASE WHEN temporary THEN 'PRESERVE' ELSE NULL END commit_action FROM duckdb_tables() UNION ALL SELECT NULL table_catalog, schema_name table_schema, view_name table_name, 'VIEW' table_type, NULL self_referencing_column_name, NULL reference_generation, NULL user_defined_type_catalog, NULL user_defined_type_schema, NULL user_defined_type_name, 'NO' is_insertable_into, 'NO' is_typed, NULL commit_action FROM duckdb_views;"},
    {nullptr, nullptr, nullptr}};

static unique_ptr<CreateViewInfo> GetDefaultView(const string &input_schema, const string &input_name) {
	auto schema = StringUtil::Lower(input_schema);
	auto name = StringUtil::Lower(input_name);
	for (idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].schema == schema && internal_views[index].name == name) {
			auto result = make_unique<CreateViewInfo>();
			result->schema = schema;
			result->sql = internal_views[index].sql;

			Parser parser;
			parser.ParseQuery(internal_views[index].sql);
			D_ASSERT(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
			result->query = unique_ptr_cast<SQLStatement, SelectStatement>(move(parser.statements[0]));
			result->temporary = true;
			result->internal = true;
			result->view_name = name;
			return result;
		}
	}
	return nullptr;
}

DefaultViewGenerator::DefaultViewGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultViewGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	auto info = GetDefaultView(schema->name, entry_name);
	if (info) {
		auto binder = Binder::CreateBinder(context);
		binder->BindCreateViewInfo(*info);

		return make_unique_base<CatalogEntry, ViewCatalogEntry>(&catalog, schema, info.get());
	}
	return nullptr;
}

vector<string> DefaultViewGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_views[index].name != nullptr; index++) {
		if (internal_views[index].schema == schema->name) {
			result.emplace_back(internal_views[index].name);
		}
	}
	return result;
}

} // namespace duckdb









namespace duckdb {

DependencyManager::DependencyManager(Catalog &catalog) : catalog(catalog) {
}

void DependencyManager::AddObject(ClientContext &context, CatalogEntry *object,
                                  unordered_set<CatalogEntry *> &dependencies) {
	// check for each object in the sources if they were not deleted yet
	for (auto &dependency : dependencies) {
		idx_t entry_index;
		CatalogEntry *catalog_entry;
		if (!dependency->set) {
			throw InternalException("Dependency has no set");
		}
		if (!dependency->set->GetEntryInternal(context, dependency->name, entry_index, catalog_entry)) {
			throw InternalException("Dependency has already been deleted?");
		}
	}
	// indexes do not require CASCADE to be dropped, they are simply always dropped along with the table
	auto dependency_type = object->type == CatalogType::INDEX_ENTRY ? DependencyType::DEPENDENCY_AUTOMATIC
	                                                                : DependencyType::DEPENDENCY_REGULAR;
	// add the object to the dependents_map of each object that it depends on
	for (auto &dependency : dependencies) {
		dependents_map[dependency].insert(Dependency(object, dependency_type));
	}
	// create the dependents map for this object: it starts out empty
	dependents_map[object] = dependency_set_t();
	dependencies_map[object] = dependencies;
}

void DependencyManager::DropObject(ClientContext &context, CatalogEntry *object, bool cascade) {
	D_ASSERT(dependents_map.find(object) != dependents_map.end());

	// first check the objects that depend on this object
	auto &dependent_objects = dependents_map[object];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		auto mapping_value = catalog_set.GetMapping(context, dep.entry->name, true /* get_latest */);
		if (mapping_value == nullptr) {
			continue;
		}
		idx_t entry_index = mapping_value->index;
		CatalogEntry *dependency_entry;

		if (!catalog_set.GetEntryInternal(context, entry_index, dependency_entry)) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		// conflict: attempting to delete this object but the dependent object still exists
		if (cascade || dep.dependency_type == DependencyType::DEPENDENCY_AUTOMATIC ||
		    dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// cascade: drop the dependent object
			catalog_set.DropEntryInternal(context, entry_index, *dependency_entry, cascade);
		} else {
			// no cascade and there are objects that depend on this object: throw error
			throw CatalogException("Cannot drop entry \"%s\" because there are entries that "
			                       "depend on it. Use DROP...CASCADE to drop all dependents.",
			                       object->name);
		}
	}
}

void DependencyManager::AlterObject(ClientContext &context, CatalogEntry *old_obj, CatalogEntry *new_obj) {
	D_ASSERT(dependents_map.find(old_obj) != dependents_map.end());
	D_ASSERT(dependencies_map.find(old_obj) != dependencies_map.end());

	// first check the objects that depend on this object
	vector<CatalogEntry *> owned_objects_to_add;
	auto &dependent_objects = dependents_map[old_obj];
	for (auto &dep : dependent_objects) {
		// look up the entry in the catalog set
		auto &catalog_set = *dep.entry->set;
		idx_t entry_index;
		CatalogEntry *dependency_entry;
		if (!catalog_set.GetEntryInternal(context, dep.entry->name, entry_index, dependency_entry)) {
			// the dependent object was already deleted, no conflict
			continue;
		}
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			// the dependent object is owned by the current object
			owned_objects_to_add.push_back(dep.entry);
			continue;
		}
		// conflict: attempting to alter this object but the dependent object still exists
		// no cascade and there are objects that depend on this object: throw error
		throw CatalogException("Cannot alter entry \"%s\" because there are entries that "
		                       "depend on it.",
		                       old_obj->name);
	}
	// add the new object to the dependents_map of each object that it depends on
	auto &old_dependencies = dependencies_map[old_obj];
	vector<CatalogEntry *> to_delete;
	for (auto &dependency : old_dependencies) {
		if (dependency->type == CatalogType::TYPE_ENTRY) {
			auto user_type = (TypeCatalogEntry *)dependency;
			auto table = (TableCatalogEntry *)new_obj;
			bool deleted_dependency = true;
			for (auto &column : table->columns) {
				if (column.Type() == user_type->user_type) {
					deleted_dependency = false;
					break;
				}
			}
			if (deleted_dependency) {
				to_delete.push_back(dependency);
				continue;
			}
		}
		dependents_map[dependency].insert(new_obj);
	}
	for (auto &dependency : to_delete) {
		old_dependencies.erase(dependency);
		dependents_map[dependency].erase(old_obj);
	}

	// We might have to add a type dependency
	vector<CatalogEntry *> to_add;
	if (new_obj->type == CatalogType::TABLE_ENTRY) {
		auto table = (TableCatalogEntry *)new_obj;
		for (auto &column : table->columns) {
			auto user_type_catalog = LogicalType::GetCatalog(column.Type());
			if (user_type_catalog) {
				to_add.push_back(user_type_catalog);
			}
		}
	}
	// add the new object to the dependency manager
	dependents_map[new_obj] = dependency_set_t();
	dependencies_map[new_obj] = old_dependencies;

	for (auto &dependency : to_add) {
		dependencies_map[new_obj].insert(dependency);
		dependents_map[dependency].insert(new_obj);
	}

	for (auto &dependency : owned_objects_to_add) {
		dependents_map[new_obj].insert(Dependency(dependency, DependencyType::DEPENDENCY_OWNS));
		dependents_map[dependency].insert(Dependency(new_obj, DependencyType::DEPENDENCY_OWNED_BY));
		dependencies_map[new_obj].insert(dependency);
	}
}

void DependencyManager::EraseObject(CatalogEntry *object) {
	// obtain the writing lock
	EraseObjectInternal(object);
}

void DependencyManager::EraseObjectInternal(CatalogEntry *object) {
	if (dependents_map.find(object) == dependents_map.end()) {
		// dependencies already removed
		return;
	}
	D_ASSERT(dependents_map.find(object) != dependents_map.end());
	D_ASSERT(dependencies_map.find(object) != dependencies_map.end());
	// now for each of the dependencies, erase the entries from the dependents_map
	for (auto &dependency : dependencies_map[object]) {
		auto entry = dependents_map.find(dependency);
		if (entry != dependents_map.end()) {
			D_ASSERT(entry->second.find(object) != entry->second.end());
			entry->second.erase(object);
		}
	}
	// erase the dependents and dependencies for this object
	dependents_map.erase(object);
	dependencies_map.erase(object);
}

void DependencyManager::Scan(const std::function<void(CatalogEntry *, CatalogEntry *, DependencyType)> &callback) {
	lock_guard<mutex> write_lock(catalog.write_lock);
	for (auto &entry : dependents_map) {
		for (auto &dependent : entry.second) {
			callback(entry.first, dependent.entry, dependent.dependency_type);
		}
	}
}

void DependencyManager::AddOwnership(ClientContext &context, CatalogEntry *owner, CatalogEntry *entry) {
	// lock the catalog for writing
	lock_guard<mutex> write_lock(catalog.write_lock);

	// If the owner is already owned by something else, throw an error
	for (auto &dep : dependents_map[owner]) {
		if (dep.dependency_type == DependencyType::DEPENDENCY_OWNED_BY) {
			throw CatalogException(owner->name + " already owned by " + dep.entry->name);
		}
	}

	// If the entry is already owned, throw an error
	for (auto &dep : dependents_map[entry]) {
		// if the entry is already owned, throw error
		if (dep.entry != owner) {
			throw CatalogException(entry->name + " already depends on " + dep.entry->name);
		}
		// if the entry owns the owner, throw error
		if (dep.entry == owner && dep.dependency_type == DependencyType::DEPENDENCY_OWNS) {
			throw CatalogException(entry->name + " already owns " + owner->name +
			                       ". Cannot have circular dependencies");
		}
	}

	// Emplace guarantees that the same object cannot be inserted twice in the unordered_set
	// In the case AddOwnership is called twice, because of emplace, the object will not be repeated in the set.
	// We use an automatic dependency because if the Owner gets deleted, then the owned objects are also deleted
	dependents_map[owner].emplace(Dependency(entry, DependencyType::DEPENDENCY_OWNS));
	dependents_map[entry].emplace(Dependency(owner, DependencyType::DEPENDENCY_OWNED_BY));
	dependencies_map[owner].emplace(entry);
}

} // namespace duckdb




#ifdef DUCKDB_DEBUG_ALLOCATION



#include <execinfo.h>
#endif

namespace duckdb {

AllocatedData::AllocatedData(Allocator &allocator, data_ptr_t pointer, idx_t allocated_size)
    : allocator(allocator), pointer(pointer), allocated_size(allocated_size) {
}
AllocatedData::~AllocatedData() {
	Reset();
}

void AllocatedData::Reset() {
	if (!pointer) {
		return;
	}
	allocator.FreeData(pointer, allocated_size);
	pointer = nullptr;
}

//===--------------------------------------------------------------------===//
// Debug Info
//===--------------------------------------------------------------------===//
struct AllocatorDebugInfo {
#ifdef DEBUG
	AllocatorDebugInfo();
	~AllocatorDebugInfo();

	static string GetStackTrace(int max_depth = 128);

	void AllocateData(data_ptr_t pointer, idx_t size);
	void FreeData(data_ptr_t pointer, idx_t size);
	void ReallocateData(data_ptr_t pointer, data_ptr_t new_pointer, idx_t old_size, idx_t new_size);

private:
	//! The number of bytes that are outstanding (i.e. that have been allocated - but not freed)
	//! Used for debug purposes
	atomic<idx_t> allocation_count;
#ifdef DUCKDB_DEBUG_ALLOCATION
	mutex pointer_lock;
	//! Set of active outstanding pointers together with stack traces
	unordered_map<data_ptr_t, pair<idx_t, string>> pointers;
#endif
#endif
};

PrivateAllocatorData::PrivateAllocatorData() {
}

PrivateAllocatorData::~PrivateAllocatorData() {
}

//===--------------------------------------------------------------------===//
// Allocator
//===--------------------------------------------------------------------===//
Allocator::Allocator()
    : Allocator(Allocator::DefaultAllocate, Allocator::DefaultFree, Allocator::DefaultReallocate, nullptr) {
}

Allocator::Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
                     reallocate_function_ptr_t reallocate_function_p, unique_ptr<PrivateAllocatorData> private_data_p)
    : allocate_function(allocate_function_p), free_function(free_function_p),
      reallocate_function(reallocate_function_p), private_data(move(private_data_p)) {
	D_ASSERT(allocate_function);
	D_ASSERT(free_function);
	D_ASSERT(reallocate_function);
#ifdef DEBUG
	if (!private_data) {
		private_data = make_unique<PrivateAllocatorData>();
	}
	private_data->debug_info = make_unique<AllocatorDebugInfo>();
#endif
}

Allocator::~Allocator() {
}

data_ptr_t Allocator::AllocateData(idx_t size) {
	auto result = allocate_function(private_data.get(), size);
#ifdef DEBUG
	D_ASSERT(private_data);
	private_data->debug_info->AllocateData(result, size);
#endif
	return result;
}

void Allocator::FreeData(data_ptr_t pointer, idx_t size) {
	if (!pointer) {
		return;
	}
#ifdef DEBUG
	D_ASSERT(private_data);
	private_data->debug_info->FreeData(pointer, size);
#endif
	free_function(private_data.get(), pointer, size);
}

data_ptr_t Allocator::ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t size) {
	if (!pointer) {
		return nullptr;
	}
	auto new_pointer = reallocate_function(private_data.get(), pointer, old_size, size);
#ifdef DEBUG
	D_ASSERT(private_data);
	private_data->debug_info->ReallocateData(pointer, new_pointer, old_size, size);
#endif
	return new_pointer;
}

Allocator &Allocator::DefaultAllocator() {
	static Allocator DEFAULT_ALLOCATOR;
	return DEFAULT_ALLOCATOR;
}

//===--------------------------------------------------------------------===//
// Debug Info (extended)
//===--------------------------------------------------------------------===//
#ifdef DEBUG
AllocatorDebugInfo::AllocatorDebugInfo() {
	allocation_count = 0;
}
AllocatorDebugInfo::~AllocatorDebugInfo() {
#ifdef DUCKDB_DEBUG_ALLOCATION
	if (allocation_count != 0) {
		printf("Outstanding allocations found for Allocator\n");
		for (auto &entry : pointers) {
			printf("Allocation of size %lld at address %p\n", entry.second.first, (void *)entry.first);
			printf("Stack trace:\n%s\n", entry.second.second.c_str());
			printf("\n");
		}
	}
#endif
	//! Verify that there is no outstanding memory still associated with the batched allocator
	//! Only works for access to the batched allocator through the batched allocator interface
	//! If this assertion triggers, enable DUCKDB_DEBUG_ALLOCATION for more information about the allocations
	D_ASSERT(allocation_count == 0);
}

string AllocatorDebugInfo::GetStackTrace(int max_depth) {
#ifdef DUCKDB_DEBUG_ALLOCATION
	string result;
	auto callstack = unique_ptr<void *[]>(new void *[max_depth]);
	int frames = backtrace(callstack.get(), max_depth);
	char **strs = backtrace_symbols(callstack.get(), frames);
	for (int i = 0; i < frames; i++) {
		result += strs[i];
		result += "\n";
	}
	free(strs);
	return result;
#else
	throw InternalException("GetStackTrace not supported without DUCKDB_DEBUG_ALLOCATION");
#endif
}

void AllocatorDebugInfo::AllocateData(data_ptr_t pointer, idx_t size) {
	allocation_count += size;
#ifdef DUCKDB_DEBUG_ALLOCATION
	lock_guard<mutex> l(pointer_lock);
	pointers[pointer] = make_pair(size, GetStackTrace());
#endif
}

void AllocatorDebugInfo::FreeData(data_ptr_t pointer, idx_t size) {
	D_ASSERT(allocation_count >= size);
	allocation_count -= size;
#ifdef DUCKDB_DEBUG_ALLOCATION
	lock_guard<mutex> l(pointer_lock);
	// verify that the pointer exists
	D_ASSERT(pointers.find(pointer) != pointers.end());
	// verify that the stored size matches the passed in size
	D_ASSERT(pointers[pointer].first == size);
	// erase the pointer
	pointers.erase(pointer);
#endif
}

void AllocatorDebugInfo::ReallocateData(data_ptr_t pointer, data_ptr_t new_pointer, idx_t old_size, idx_t new_size) {
	FreeData(pointer, old_size);
	AllocateData(new_pointer, new_size);
}

#endif

} // namespace duckdb










namespace duckdb {

ArrowSchemaWrapper::~ArrowSchemaWrapper() {
	if (arrow_schema.release) {
		for (int64_t child_idx = 0; child_idx < arrow_schema.n_children; child_idx++) {
			auto &child = *arrow_schema.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_schema.release(&arrow_schema);
		arrow_schema.release = nullptr;
	}
}

ArrowArrayWrapper::~ArrowArrayWrapper() {
	if (arrow_array.release) {
		for (int64_t child_idx = 0; child_idx < arrow_array.n_children; child_idx++) {
			auto &child = *arrow_array.children[child_idx];
			if (child.release) {
				child.release(&child);
			}
		}
		arrow_array.release(&arrow_array);
		arrow_array.release = nullptr;
	}
}

ArrowArrayStreamWrapper::~ArrowArrayStreamWrapper() {
	if (arrow_array_stream.release) {
		arrow_array_stream.release(&arrow_array_stream);
		arrow_array_stream.release = nullptr;
	}
}

void ArrowArrayStreamWrapper::GetSchema(ArrowSchemaWrapper &schema) {
	D_ASSERT(arrow_array_stream.get_schema);
	// LCOV_EXCL_START
	if (arrow_array_stream.get_schema(&arrow_array_stream, &schema.arrow_schema)) {
		throw InvalidInputException("arrow_scan: get_schema failed(): %s", string(GetError()));
	}
	if (!schema.arrow_schema.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}
	if (schema.arrow_schema.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}
	// LCOV_EXCL_STOP
}

shared_ptr<ArrowArrayWrapper> ArrowArrayStreamWrapper::GetNextChunk() {
	auto current_chunk = make_shared<ArrowArrayWrapper>();
	if (arrow_array_stream.get_next(&arrow_array_stream, &current_chunk->arrow_array)) { // LCOV_EXCL_START
		throw InvalidInputException("arrow_scan: get_next failed(): %s", string(GetError()));
	} // LCOV_EXCL_STOP

	return current_chunk;
}

const char *ArrowArrayStreamWrapper::GetError() { // LCOV_EXCL_START
	return arrow_array_stream.get_last_error(&arrow_array_stream);
} // LCOV_EXCL_STOP

int ResultArrowArrayStreamWrapper::MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
	if (!stream->release) {
		return -1;
	}
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	if (!my_stream->column_types.empty()) {
		QueryResult::ToArrowSchema(out, my_stream->column_types, my_stream->column_names, my_stream->timezone_config);
		return 0;
	}

	auto &result = *my_stream->result;
	if (!result.success) {
		my_stream->last_error = "Query Failed";
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = (StreamQueryResult &)result;
		if (!stream_result.IsOpen()) {
			my_stream->last_error = "Query Stream is closed";
			return -1;
		}
	}
	if (my_stream->column_types.empty()) {
		my_stream->column_types = result.types;
		my_stream->column_names = result.names;
	}
	QueryResult::ToArrowSchema(out, my_stream->column_types, my_stream->column_names, my_stream->timezone_config);
	return 0;
}

int ResultArrowArrayStreamWrapper::MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
	if (!stream->release) {
		return -1;
	}
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	auto &result = *my_stream->result;
	if (!result.success) {
		my_stream->last_error = "Query Failed";
		return -1;
	}
	if (result.type == QueryResultType::STREAM_RESULT) {
		auto &stream_result = (StreamQueryResult &)result;
		if (!stream_result.IsOpen()) {
			// Nothing to output
			out->release = nullptr;
			return 0;
		}
	}
	if (my_stream->column_types.empty()) {
		my_stream->column_types = result.types;
		my_stream->column_names = result.names;
	}
	unique_ptr<DataChunk> chunk_result = result.Fetch();
	if (!chunk_result) {
		// Nothing to output
		out->release = nullptr;
		return 0;
	}
	unique_ptr<DataChunk> agg_chunk_result = make_unique<DataChunk>();
	agg_chunk_result->Initialize(Allocator::DefaultAllocator(), chunk_result->GetTypes());
	agg_chunk_result->Append(*chunk_result, true);

	while (agg_chunk_result->size() < my_stream->batch_size) {
		auto new_chunk = result.Fetch();
		if (!new_chunk) {
			break;
		} else {
			agg_chunk_result->Append(*new_chunk, true);
		}
	}
	agg_chunk_result->ToArrowArray(out);
	return 0;
}

void ResultArrowArrayStreamWrapper::MyStreamRelease(struct ArrowArrayStream *stream) {
	if (!stream->release) {
		return;
	}
	stream->release = nullptr;
	delete (ResultArrowArrayStreamWrapper *)stream->private_data;
}

const char *ResultArrowArrayStreamWrapper::MyStreamGetLastError(struct ArrowArrayStream *stream) {
	if (!stream->release) {
		return "stream was released";
	}
	D_ASSERT(stream->private_data);
	auto my_stream = (ResultArrowArrayStreamWrapper *)stream->private_data;
	return my_stream->last_error.c_str();
}
ResultArrowArrayStreamWrapper::ResultArrowArrayStreamWrapper(unique_ptr<QueryResult> result_p, idx_t batch_size_p)
    : result(move(result_p)) {
	//! We first initialize the private data of the stream
	stream.private_data = this;
	//! Ceil Approx_Batch_Size/STANDARD_VECTOR_SIZE
	if (batch_size_p == 0) {
		throw std::runtime_error("Approximate Batch Size of Record Batch MUST be higher than 0");
	}
	batch_size = batch_size_p;
	//! We initialize the stream functions
	stream.get_schema = ResultArrowArrayStreamWrapper::MyStreamGetSchema;
	stream.get_next = ResultArrowArrayStreamWrapper::MyStreamGetNext;
	stream.release = ResultArrowArrayStreamWrapper::MyStreamRelease;
	stream.get_last_error = ResultArrowArrayStreamWrapper::MyStreamGetLastError;
}

unique_ptr<DataChunk> ArrowUtil::FetchNext(QueryResult &result) {
	auto chunk = result.Fetch();
	if (!result.success) {
		throw std::runtime_error(result.error);
	}
	return chunk;
}

unique_ptr<DataChunk> ArrowUtil::FetchChunk(QueryResult *result, idx_t chunk_size) {

	auto data_chunk = FetchNext(*result);
	if (!data_chunk) {
		return data_chunk;
	}
	while (data_chunk->size() < chunk_size) {
		auto next_chunk = FetchNext(*result);
		if (!next_chunk || next_chunk->size() == 0) {
			break;
		}
		data_chunk->Append(*next_chunk, true);
	}
	return data_chunk;
}

} // namespace duckdb



namespace duckdb {

void DuckDBAssertInternal(bool condition, const char *condition_name, const char *file, int linenr) {
	if (condition) {
		return;
	}
	throw InternalException("Assertion triggered in file \"%s\" on line %d: %s", file, linenr, condition_name);
}

} // namespace duckdb



namespace duckdb {

uint64_t Checksum(uint8_t *buffer, size_t size) {
	uint64_t result = 5381;
	uint64_t *ptr = (uint64_t *)buffer;
	size_t i;
	// for efficiency, we first hash uint64_t values
	for (i = 0; i < size / 8; i++) {
		result ^= Hash(ptr[i]);
	}
	if (size - i * 8 > 0) {
		// the remaining 0-7 bytes we hash using a string hash
		result ^= Hash(buffer + i * 8, size - i * 8);
	}
	return result;
}

} // namespace duckdb


namespace duckdb {

StreamWrapper::~StreamWrapper() {
}

CompressedFile::CompressedFile(CompressedFileSystem &fs, unique_ptr<FileHandle> child_handle_p, const string &path)
    : FileHandle(fs, path), compressed_fs(fs), child_handle(move(child_handle_p)) {
}

CompressedFile::~CompressedFile() {
	Close();
}

void CompressedFile::Initialize(bool write) {
	Close();

	this->write = write;
	stream_data.in_buf_size = compressed_fs.InBufferSize();
	stream_data.out_buf_size = compressed_fs.OutBufferSize();
	stream_data.in_buff = unique_ptr<data_t[]>(new data_t[stream_data.in_buf_size]);
	stream_data.in_buff_start = stream_data.in_buff.get();
	stream_data.in_buff_end = stream_data.in_buff.get();
	stream_data.out_buff = unique_ptr<data_t[]>(new data_t[stream_data.out_buf_size]);
	stream_data.out_buff_start = stream_data.out_buff.get();
	stream_data.out_buff_end = stream_data.out_buff.get();

	stream_wrapper = compressed_fs.CreateStream();
	stream_wrapper->Initialize(*this, write);
}

int64_t CompressedFile::ReadData(void *buffer, int64_t remaining) {
	idx_t total_read = 0;
	while (true) {
		// first check if there are input bytes available in the output buffers
		if (stream_data.out_buff_start != stream_data.out_buff_end) {
			// there is! copy it into the output buffer
			idx_t available = MinValue<idx_t>(remaining, stream_data.out_buff_end - stream_data.out_buff_start);
			memcpy(data_ptr_t(buffer) + total_read, stream_data.out_buff_start, available);

			// increment the total read variables as required
			stream_data.out_buff_start += available;
			total_read += available;
			remaining -= available;
			if (remaining == 0) {
				// done! read enough
				return total_read;
			}
		}
		if (!stream_wrapper) {
			return total_read;
		}

		// ran out of buffer: read more data from the child stream
		stream_data.out_buff_start = stream_data.out_buff.get();
		stream_data.out_buff_end = stream_data.out_buff.get();
		D_ASSERT(stream_data.in_buff_start <= stream_data.in_buff_end);
		D_ASSERT(stream_data.in_buff_end <= stream_data.in_buff_start + stream_data.in_buf_size);

		// read more input if none available
		if (stream_data.in_buff_start == stream_data.in_buff_end) {
			// empty input buffer: refill from the start
			stream_data.in_buff_start = stream_data.in_buff.get();
			stream_data.in_buff_end = stream_data.in_buff_start;
			auto sz = child_handle->Read(stream_data.in_buff.get(), stream_data.in_buf_size);
			if (sz <= 0) {
				stream_wrapper.reset();
				break;
			}
			stream_data.in_buff_end = stream_data.in_buff_start + sz;
		}

		auto finished = stream_wrapper->Read(stream_data);
		if (finished) {
			stream_wrapper.reset();
		}
	}
	return total_read;
}

int64_t CompressedFile::WriteData(data_ptr_t buffer, int64_t nr_bytes) {
	stream_wrapper->Write(*this, stream_data, buffer, nr_bytes);
	return nr_bytes;
}

void CompressedFile::Close() {
	if (stream_wrapper) {
		stream_wrapper->Close();
		stream_wrapper.reset();
	}
	stream_data.in_buff.reset();
	stream_data.out_buff.reset();
	stream_data.out_buff_start = nullptr;
	stream_data.out_buff_end = nullptr;
	stream_data.in_buff_start = nullptr;
	stream_data.in_buff_end = nullptr;
	stream_data.in_buf_size = 0;
	stream_data.out_buf_size = 0;
}

int64_t CompressedFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.ReadData(buffer, nr_bytes);
}

int64_t CompressedFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.WriteData((data_ptr_t)buffer, nr_bytes);
}

void CompressedFileSystem::Reset(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	compressed_file.child_handle->Reset();
	compressed_file.Initialize(compressed_file.write);
}

int64_t CompressedFileSystem::GetFileSize(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.child_handle->GetFileSize();
}

bool CompressedFileSystem::OnDiskFile(FileHandle &handle) {
	auto &compressed_file = (CompressedFile &)handle;
	return compressed_file.child_handle->OnDiskFile();
}

bool CompressedFileSystem::CanSeek() {
	return false;
}

} // namespace duckdb




namespace duckdb {

constexpr const idx_t DConstants::INVALID_INDEX;
const row_t MAX_ROW_ID = 4611686018427388000ULL; // 2^62
const column_t COLUMN_IDENTIFIER_ROW_ID = (column_t)-1;
const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};
const double PI = 3.141592653589793;

const transaction_t TRANSACTION_ID_START = 4611686018427388000ULL;                // 2^62
const transaction_t MAX_TRANSACTION_ID = NumericLimits<transaction_t>::Maximum(); // 2^63
const transaction_t NOT_DELETED_ID = NumericLimits<transaction_t>::Maximum() - 1; // 2^64 - 1
const transaction_t MAXIMUM_QUERY_ID = NumericLimits<transaction_t>::Maximum();   // 2^64

uint64_t NextPowerOfTwo(uint64_t v) {
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	v++;
	return v;
}

} // namespace duckdb
/*
** This code taken from the SQLite test library.  Originally found on
** the internet.  The original header comment follows this comment.
** The code is largerly unchanged, but there have been some modifications.
*/
/*
 * This code implements the MD5 message-digest algorithm.
 * The algorithm is due to Ron Rivest.  This code was
 * written by Colin Plumb in 1993, no copyright is claimed.
 * This code is in the public domain; do with it what you wish.
 *
 * Equivalent code is available from RSA Data Security, Inc.
 * This code has been tested against that, and is equivalent,
 * except that you don't need to include two pages of legalese
 * with every copy.
 *
 * To compute the message digest of a chunk of bytes, declare an
 * MD5Context structure, pass it to MD5Init, call MD5Update as
 * needed on buffers full of bytes, and then call MD5Final, which
 * will fill a supplied 16-byte array with the digest.
 */


namespace duckdb {

/*
 * Note: this code is harmless on little-endian machines.
 */
static void ByteReverse(unsigned char *buf, unsigned longs) {
	uint32_t t;
	do {
		t = (uint32_t)((unsigned)buf[3] << 8 | buf[2]) << 16 | ((unsigned)buf[1] << 8 | buf[0]);
		*(uint32_t *)buf = t;
		buf += 4;
	} while (--longs);
}
/* The four core functions - F1 is optimized somewhat */

/* #define F1(x, y, z) (x & y | ~x & z) */
#define F1(x, y, z) ((z) ^ ((x) & ((y) ^ (z))))
#define F2(x, y, z) F1(z, x, y)
#define F3(x, y, z) ((x) ^ (y) ^ (z))
#define F4(x, y, z) ((y) ^ ((x) | ~(z)))

/* This is the central step in the MD5 algorithm. */
#define MD5STEP(f, w, x, y, z, data, s) ((w) += f(x, y, z) + (data), (w) = (w) << (s) | (w) >> (32 - (s)), (w) += (x))

/*
 * The core of the MD5 algorithm, this alters an existing MD5 hash to
 * reflect the addition of 16 longwords of new data.  MD5Update blocks
 * the data and converts bytes into longwords for this routine.
 */
static void MD5Transform(uint32_t buf[4], const uint32_t in[16]) {
	uint32_t a, b, c, d;

	a = buf[0];
	b = buf[1];
	c = buf[2];
	d = buf[3];

	MD5STEP(F1, a, b, c, d, in[0] + 0xd76aa478, 7);
	MD5STEP(F1, d, a, b, c, in[1] + 0xe8c7b756, 12);
	MD5STEP(F1, c, d, a, b, in[2] + 0x242070db, 17);
	MD5STEP(F1, b, c, d, a, in[3] + 0xc1bdceee, 22);
	MD5STEP(F1, a, b, c, d, in[4] + 0xf57c0faf, 7);
	MD5STEP(F1, d, a, b, c, in[5] + 0x4787c62a, 12);
	MD5STEP(F1, c, d, a, b, in[6] + 0xa8304613, 17);
	MD5STEP(F1, b, c, d, a, in[7] + 0xfd469501, 22);
	MD5STEP(F1, a, b, c, d, in[8] + 0x698098d8, 7);
	MD5STEP(F1, d, a, b, c, in[9] + 0x8b44f7af, 12);
	MD5STEP(F1, c, d, a, b, in[10] + 0xffff5bb1, 17);
	MD5STEP(F1, b, c, d, a, in[11] + 0x895cd7be, 22);
	MD5STEP(F1, a, b, c, d, in[12] + 0x6b901122, 7);
	MD5STEP(F1, d, a, b, c, in[13] + 0xfd987193, 12);
	MD5STEP(F1, c, d, a, b, in[14] + 0xa679438e, 17);
	MD5STEP(F1, b, c, d, a, in[15] + 0x49b40821, 22);

	MD5STEP(F2, a, b, c, d, in[1] + 0xf61e2562, 5);
	MD5STEP(F2, d, a, b, c, in[6] + 0xc040b340, 9);
	MD5STEP(F2, c, d, a, b, in[11] + 0x265e5a51, 14);
	MD5STEP(F2, b, c, d, a, in[0] + 0xe9b6c7aa, 20);
	MD5STEP(F2, a, b, c, d, in[5] + 0xd62f105d, 5);
	MD5STEP(F2, d, a, b, c, in[10] + 0x02441453, 9);
	MD5STEP(F2, c, d, a, b, in[15] + 0xd8a1e681, 14);
	MD5STEP(F2, b, c, d, a, in[4] + 0xe7d3fbc8, 20);
	MD5STEP(F2, a, b, c, d, in[9] + 0x21e1cde6, 5);
	MD5STEP(F2, d, a, b, c, in[14] + 0xc33707d6, 9);
	MD5STEP(F2, c, d, a, b, in[3] + 0xf4d50d87, 14);
	MD5STEP(F2, b, c, d, a, in[8] + 0x455a14ed, 20);
	MD5STEP(F2, a, b, c, d, in[13] + 0xa9e3e905, 5);
	MD5STEP(F2, d, a, b, c, in[2] + 0xfcefa3f8, 9);
	MD5STEP(F2, c, d, a, b, in[7] + 0x676f02d9, 14);
	MD5STEP(F2, b, c, d, a, in[12] + 0x8d2a4c8a, 20);

	MD5STEP(F3, a, b, c, d, in[5] + 0xfffa3942, 4);
	MD5STEP(F3, d, a, b, c, in[8] + 0x8771f681, 11);
	MD5STEP(F3, c, d, a, b, in[11] + 0x6d9d6122, 16);
	MD5STEP(F3, b, c, d, a, in[14] + 0xfde5380c, 23);
	MD5STEP(F3, a, b, c, d, in[1] + 0xa4beea44, 4);
	MD5STEP(F3, d, a, b, c, in[4] + 0x4bdecfa9, 11);
	MD5STEP(F3, c, d, a, b, in[7] + 0xf6bb4b60, 16);
	MD5STEP(F3, b, c, d, a, in[10] + 0xbebfbc70, 23);
	MD5STEP(F3, a, b, c, d, in[13] + 0x289b7ec6, 4);
	MD5STEP(F3, d, a, b, c, in[0] + 0xeaa127fa, 11);
	MD5STEP(F3, c, d, a, b, in[3] + 0xd4ef3085, 16);
	MD5STEP(F3, b, c, d, a, in[6] + 0x04881d05, 23);
	MD5STEP(F3, a, b, c, d, in[9] + 0xd9d4d039, 4);
	MD5STEP(F3, d, a, b, c, in[12] + 0xe6db99e5, 11);
	MD5STEP(F3, c, d, a, b, in[15] + 0x1fa27cf8, 16);
	MD5STEP(F3, b, c, d, a, in[2] + 0xc4ac5665, 23);

	MD5STEP(F4, a, b, c, d, in[0] + 0xf4292244, 6);
	MD5STEP(F4, d, a, b, c, in[7] + 0x432aff97, 10);
	MD5STEP(F4, c, d, a, b, in[14] + 0xab9423a7, 15);
	MD5STEP(F4, b, c, d, a, in[5] + 0xfc93a039, 21);
	MD5STEP(F4, a, b, c, d, in[12] + 0x655b59c3, 6);
	MD5STEP(F4, d, a, b, c, in[3] + 0x8f0ccc92, 10);
	MD5STEP(F4, c, d, a, b, in[10] + 0xffeff47d, 15);
	MD5STEP(F4, b, c, d, a, in[1] + 0x85845dd1, 21);
	MD5STEP(F4, a, b, c, d, in[8] + 0x6fa87e4f, 6);
	MD5STEP(F4, d, a, b, c, in[15] + 0xfe2ce6e0, 10);
	MD5STEP(F4, c, d, a, b, in[6] + 0xa3014314, 15);
	MD5STEP(F4, b, c, d, a, in[13] + 0x4e0811a1, 21);
	MD5STEP(F4, a, b, c, d, in[4] + 0xf7537e82, 6);
	MD5STEP(F4, d, a, b, c, in[11] + 0xbd3af235, 10);
	MD5STEP(F4, c, d, a, b, in[2] + 0x2ad7d2bb, 15);
	MD5STEP(F4, b, c, d, a, in[9] + 0xeb86d391, 21);

	buf[0] += a;
	buf[1] += b;
	buf[2] += c;
	buf[3] += d;
}

/*
 * Start MD5 accumulation.  Set bit count to 0 and buffer to mysterious
 * initialization constants.
 */
MD5Context::MD5Context() {
	buf[0] = 0x67452301;
	buf[1] = 0xefcdab89;
	buf[2] = 0x98badcfe;
	buf[3] = 0x10325476;
	bits[0] = 0;
	bits[1] = 0;
}

/*
 * Update context to reflect the concatenation of another buffer full
 * of bytes.
 */
void MD5Context::MD5Update(const_data_ptr_t input, idx_t len) {
	uint32_t t;

	/* Update bitcount */

	t = bits[0];
	if ((bits[0] = t + ((uint32_t)len << 3)) < t) {
		bits[1]++; /* Carry from low to high */
	}
	bits[1] += len >> 29;

	t = (t >> 3) & 0x3f; /* Bytes already in shsInfo->data */

	/* Handle any leading odd-sized chunks */

	if (t) {
		unsigned char *p = (unsigned char *)in + t;

		t = 64 - t;
		if (len < t) {
			memcpy(p, input, len);
			return;
		}
		memcpy(p, input, t);
		ByteReverse(in, 16);
		MD5Transform(buf, (uint32_t *)in);
		input += t;
		len -= t;
	}

	/* Process data in 64-byte chunks */

	while (len >= 64) {
		memcpy(in, input, 64);
		ByteReverse(in, 16);
		MD5Transform(buf, (uint32_t *)in);
		input += 64;
		len -= 64;
	}

	/* Handle any remaining bytes of data. */
	memcpy(in, input, len);
}

/*
 * Final wrapup - pad to 64-byte boundary with the bit pattern
 * 1 0* (64-bit count of bits processed, MSB-first)
 */
void MD5Context::Finish(data_ptr_t out_digest) {
	unsigned count;
	unsigned char *p;

	/* Compute number of bytes mod 64 */
	count = (bits[0] >> 3) & 0x3F;

	/* Set the first char of padding to 0x80.  This is safe since there is
	   always at least one byte free */
	p = in + count;
	*p++ = 0x80;

	/* Bytes of padding needed to make 64 bytes */
	count = 64 - 1 - count;

	/* Pad out to 56 mod 64 */
	if (count < 8) {
		/* Two lots of padding:  Pad the first block to 64 bytes */
		memset(p, 0, count);
		ByteReverse(in, 16);
		MD5Transform(buf, (uint32_t *)in);

		/* Now fill the next block with 56 bytes */
		memset(in, 0, 56);
	} else {
		/* Pad block to 56 bytes */
		memset(p, 0, count - 8);
	}
	ByteReverse(in, 14);

	/* Append length in bits and transform */
	((uint32_t *)in)[14] = bits[0];
	((uint32_t *)in)[15] = bits[1];

	MD5Transform(buf, (uint32_t *)in);
	ByteReverse((unsigned char *)buf, 4);
	memcpy(out_digest, buf, 16);
}

void MD5Context::DigestToBase16(const_data_ptr_t digest, char *zbuf) {
	static char const HEX_CODES[] = "0123456789abcdef";
	int i, j;

	for (j = i = 0; i < 16; i++) {
		int a = digest[i];
		zbuf[j++] = HEX_CODES[(a >> 4) & 0xf];
		zbuf[j++] = HEX_CODES[a & 0xf];
	}
}

void MD5Context::FinishHex(char *out_digest) {
	data_t digest[MD5_HASH_LENGTH_BINARY];
	Finish(digest);
	DigestToBase16(digest, out_digest);
}

string MD5Context::FinishHex() {
	char digest[MD5_HASH_LENGTH_TEXT];
	FinishHex(digest);
	return string(digest, MD5_HASH_LENGTH_TEXT);
}

void MD5Context::Add(const char *data) {
	MD5Update((const_data_ptr_t)data, strlen(data));
}

} // namespace duckdb
// This file is licensed under Apache License 2.0
// Source code taken from https://github.com/google/benchmark
// It is highly modified




namespace duckdb {

inline uint64_t ChronoNow() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(
	           std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now())
	               .time_since_epoch())
	    .count();
}

inline uint64_t Now() {
#if defined(RDTSC)
#if defined(__i386__)
	uint64_t ret;
	__asm__ volatile("rdtsc" : "=A"(ret));
	return ret;
#elif defined(__x86_64__) || defined(__amd64__)
	uint64_t low, high;
	__asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
	return (high << 32) | low;
#elif defined(__powerpc__) || defined(__ppc__)
	uint64_t tbl, tbu0, tbu1;
	asm("mftbu %0" : "=r"(tbu0));
	asm("mftb  %0" : "=r"(tbl));
	asm("mftbu %0" : "=r"(tbu1));
	tbl &= -static_cast<int64>(tbu0 == tbu1);
	return (tbu1 << 32) | tbl;
#elif defined(__sparc__)
	uint64_t tick;
	asm(".byte 0x83, 0x41, 0x00, 0x00");
	asm("mov   %%g1, %0" : "=r"(tick));
	return tick;
#elif defined(__ia64__)
	uint64_t itc;
	asm("mov %0 = ar.itc" : "=r"(itc));
	return itc;
#elif defined(COMPILER_MSVC) && defined(_M_IX86)
	_asm rdtsc
#elif defined(COMPILER_MSVC)
	return __rdtsc();
#elif defined(__aarch64__)
	uint64_t virtual_timer_value;
	asm volatile("mrs %0, cntvct_el0" : "=r"(virtual_timer_value));
	return virtual_timer_value;
#elif defined(__ARM_ARCH)
#if (__ARM_ARCH >= 6)
	uint32_t pmccntr;
	uint32_t pmuseren;
	uint32_t pmcntenset;
	asm volatile("mrc p15, 0, %0, c9, c14, 0" : "=r"(pmuseren));
	if (pmuseren & 1) { // Allows reading perfmon counters for user mode code.
		asm volatile("mrc p15, 0, %0, c9, c12, 1" : "=r"(pmcntenset));
		if (pmcntenset & 0x80000000ul) { // Is it counting?
			asm volatile("mrc p15, 0, %0, c9, c13, 0" : "=r"(pmccntr));
			return static_cast<uint64_t>(pmccntr) * 64; // Should optimize to << 6
		}
	}
#endif
	return ChronoNow();
#else
	return ChronoNow();
#endif
#else
	return ChronoNow();
#endif // defined(RDTSC)
}
uint64_t CycleCounter::Tick() const {
	return Now();
}
} // namespace duckdb




namespace duckdb {

// LCOV_EXCL_START
string CatalogTypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::COLLATION_ENTRY:
		return "Collation";
	case CatalogType::TYPE_ENTRY:
		return "Type";
	case CatalogType::TABLE_ENTRY:
		return "Table";
	case CatalogType::SCHEMA_ENTRY:
		return "Schema";
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return "Table Function";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "Scalar Function";
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return "Aggregate Function";
	case CatalogType::COPY_FUNCTION_ENTRY:
		return "Copy Function";
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return "Pragma Function";
	case CatalogType::MACRO_ENTRY:
		return "Macro Function";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "Table Macro Function";
	case CatalogType::VIEW_ENTRY:
		return "View";
	case CatalogType::INDEX_ENTRY:
		return "Index";
	case CatalogType::PREPARED_STATEMENT:
		return "Prepared Statement";
	case CatalogType::SEQUENCE_ENTRY:
		return "Sequence";
	case CatalogType::INVALID:
	case CatalogType::DELETED_ENTRY:
	case CatalogType::UPDATED_ENTRY:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

// LCOV_EXCL_START
CompressionType CompressionTypeFromString(const string &str) {
	auto compression = StringUtil::Lower(str);
	if (compression == "uncompressed") {
		return CompressionType::COMPRESSION_UNCOMPRESSED;
	} else if (compression == "rle") {
		return CompressionType::COMPRESSION_RLE;
	} else if (compression == "dictionary") {
		return CompressionType::COMPRESSION_DICTIONARY;
	} else if (compression == "pfor") {
		return CompressionType::COMPRESSION_PFOR_DELTA;
	} else if (compression == "bitpacking") {
		return CompressionType::COMPRESSION_BITPACKING;
	} else if (compression == "fsst") {
		return CompressionType::COMPRESSION_FSST;
	} else {
		return CompressionType::COMPRESSION_AUTO;
	}
}

string CompressionTypeToString(CompressionType type) {
	switch (type) {
	case CompressionType::COMPRESSION_UNCOMPRESSED:
		return "Uncompressed";
	case CompressionType::COMPRESSION_CONSTANT:
		return "Constant";
	case CompressionType::COMPRESSION_RLE:
		return "RLE";
	case CompressionType::COMPRESSION_DICTIONARY:
		return "Dictionary";
	case CompressionType::COMPRESSION_PFOR_DELTA:
		return "PFOR";
	case CompressionType::COMPRESSION_BITPACKING:
		return "BitPacking";
	case CompressionType::COMPRESSION_FSST:
		return "FSST";
	default:
		throw InternalException("Unrecognized compression type!");
	}
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

// LCOV_EXCL_START
string ExpressionTypeToString(ExpressionType type) {
	switch (type) {
	case ExpressionType::OPERATOR_CAST:
		return "CAST";
	case ExpressionType::OPERATOR_NOT:
		return "NOT";
	case ExpressionType::OPERATOR_IS_NULL:
		return "IS_NULL";
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return "IS_NOT_NULL";
	case ExpressionType::COMPARE_EQUAL:
		return "EQUAL";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "NOTEQUAL";
	case ExpressionType::COMPARE_LESSTHAN:
		return "LESSTHAN";
	case ExpressionType::COMPARE_GREATERTHAN:
		return "GREATERTHAN";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "LESSTHANOREQUALTO";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return "GREATERTHANOREQUALTO";
	case ExpressionType::COMPARE_IN:
		return "IN";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "DISTINCT_FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "NOT_DISTINCT_FROM";
	case ExpressionType::CONJUNCTION_AND:
		return "AND";
	case ExpressionType::CONJUNCTION_OR:
		return "OR";
	case ExpressionType::VALUE_CONSTANT:
		return "CONSTANT";
	case ExpressionType::VALUE_PARAMETER:
		return "PARAMETER";
	case ExpressionType::VALUE_TUPLE:
		return "TUPLE";
	case ExpressionType::VALUE_TUPLE_ADDRESS:
		return "TUPLE_ADDRESS";
	case ExpressionType::VALUE_NULL:
		return "NULL";
	case ExpressionType::VALUE_VECTOR:
		return "VECTOR";
	case ExpressionType::VALUE_SCALAR:
		return "SCALAR";
	case ExpressionType::AGGREGATE:
		return "AGGREGATE";
	case ExpressionType::WINDOW_AGGREGATE:
		return "WINDOW_AGGREGATE";
	case ExpressionType::WINDOW_RANK:
		return "RANK";
	case ExpressionType::WINDOW_RANK_DENSE:
		return "RANK_DENSE";
	case ExpressionType::WINDOW_PERCENT_RANK:
		return "PERCENT_RANK";
	case ExpressionType::WINDOW_ROW_NUMBER:
		return "ROW_NUMBER";
	case ExpressionType::WINDOW_FIRST_VALUE:
		return "FIRST_VALUE";
	case ExpressionType::WINDOW_LAST_VALUE:
		return "LAST_VALUE";
	case ExpressionType::WINDOW_NTH_VALUE:
		return "NTH_VALUE";
	case ExpressionType::WINDOW_CUME_DIST:
		return "CUME_DIST";
	case ExpressionType::WINDOW_LEAD:
		return "LEAD";
	case ExpressionType::WINDOW_LAG:
		return "LAG";
	case ExpressionType::WINDOW_NTILE:
		return "NTILE";
	case ExpressionType::FUNCTION:
		return "FUNCTION";
	case ExpressionType::CASE_EXPR:
		return "CASE";
	case ExpressionType::OPERATOR_NULLIF:
		return "NULLIF";
	case ExpressionType::OPERATOR_COALESCE:
		return "COALESCE";
	case ExpressionType::ARRAY_EXTRACT:
		return "ARRAY_EXTRACT";
	case ExpressionType::ARRAY_SLICE:
		return "ARRAY_SLICE";
	case ExpressionType::STRUCT_EXTRACT:
		return "STRUCT_EXTRACT";
	case ExpressionType::SUBQUERY:
		return "SUBQUERY";
	case ExpressionType::STAR:
		return "STAR";
	case ExpressionType::PLACEHOLDER:
		return "PLACEHOLDER";
	case ExpressionType::COLUMN_REF:
		return "COLUMN_REF";
	case ExpressionType::FUNCTION_REF:
		return "FUNCTION_REF";
	case ExpressionType::TABLE_REF:
		return "TABLE_REF";
	case ExpressionType::CAST:
		return "CAST";
	case ExpressionType::COMPARE_NOT_IN:
		return "COMPARE_NOT_IN";
	case ExpressionType::COMPARE_BETWEEN:
		return "COMPARE_BETWEEN";
	case ExpressionType::COMPARE_NOT_BETWEEN:
		return "COMPARE_NOT_BETWEEN";
	case ExpressionType::VALUE_DEFAULT:
		return "VALUE_DEFAULT";
	case ExpressionType::BOUND_REF:
		return "BOUND_REF";
	case ExpressionType::BOUND_COLUMN_REF:
		return "BOUND_COLUMN_REF";
	case ExpressionType::BOUND_FUNCTION:
		return "BOUND_FUNCTION";
	case ExpressionType::BOUND_AGGREGATE:
		return "BOUND_AGGREGATE";
	case ExpressionType::GROUPING_FUNCTION:
		return "GROUPING";
	case ExpressionType::ARRAY_CONSTRUCTOR:
		return "ARRAY_CONSTRUCTOR";
	case ExpressionType::TABLE_STAR:
		return "TABLE_STAR";
	case ExpressionType::BOUND_UNNEST:
		return "BOUND_UNNEST";
	case ExpressionType::COLLATE:
		return "COLLATE";
	case ExpressionType::POSITIONAL_REFERENCE:
		return "POSITIONAL_REFERENCE";
	case ExpressionType::LAMBDA:
		return "LAMBDA";
	case ExpressionType::ARROW:
		return "ARROW";
	case ExpressionType::INVALID:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

string ExpressionTypeToOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return "=";
	case ExpressionType::COMPARE_NOTEQUAL:
		return "!=";
	case ExpressionType::COMPARE_LESSTHAN:
		return "<";
	case ExpressionType::COMPARE_GREATERTHAN:
		return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ">=";
	case ExpressionType::COMPARE_DISTINCT_FROM:
		return "IS DISTINCT FROM";
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		return "IS NOT DISTINCT FROM";
	case ExpressionType::CONJUNCTION_AND:
		return "AND";
	case ExpressionType::CONJUNCTION_OR:
		return "OR";
	default:
		return "";
	}
}

ExpressionType NegateComparisionExpression(ExpressionType type) {
	ExpressionType negated_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		negated_type = ExpressionType::COMPARE_NOTEQUAL;
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		negated_type = ExpressionType::COMPARE_EQUAL;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		negated_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		negated_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		negated_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	default:
		throw InternalException("Unsupported comparison type in negation");
	}
	return negated_type;
}

ExpressionType FlipComparisionExpression(ExpressionType type) {
	ExpressionType flipped_type = ExpressionType::INVALID;
	switch (type) {
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
	case ExpressionType::COMPARE_DISTINCT_FROM:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_EQUAL:
		flipped_type = type;
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		flipped_type = ExpressionType::COMPARE_GREATERTHAN;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		flipped_type = ExpressionType::COMPARE_LESSTHAN;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_GREATERTHANOREQUALTO;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		flipped_type = ExpressionType::COMPARE_LESSTHANOREQUALTO;
		break;
	default:
		throw InternalException("Unsupported comparison type in flip");
	}
	return flipped_type;
}

ExpressionType OperatorToExpressionType(const string &op) {
	if (op == "=" || op == "==") {
		return ExpressionType::COMPARE_EQUAL;
	} else if (op == "!=" || op == "<>") {
		return ExpressionType::COMPARE_NOTEQUAL;
	} else if (op == "<") {
		return ExpressionType::COMPARE_LESSTHAN;
	} else if (op == ">") {
		return ExpressionType::COMPARE_GREATERTHAN;
	} else if (op == "<=") {
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	} else if (op == ">=") {
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	}
	return ExpressionType::INVALID;
}

} // namespace duckdb



namespace duckdb {

FileCompressionType FileCompressionTypeFromString(const string &input) {
	auto parameter = StringUtil::Lower(input);
	if (parameter == "infer" || parameter == "auto") {
		return FileCompressionType::AUTO_DETECT;
	} else if (parameter == "gzip") {
		return FileCompressionType::GZIP;
	} else if (parameter == "zstd") {
		return FileCompressionType::ZSTD;
	} else if (parameter == "uncompressed" || parameter == "none" || parameter.empty()) {
		return FileCompressionType::UNCOMPRESSED;
	} else {
		throw ParserException("Unrecognized file compression type \"%s\"", input);
	}
}

} // namespace duckdb


namespace duckdb {

string JoinTypeToString(JoinType type) {
	switch (type) {
	case JoinType::LEFT:
		return "LEFT";
	case JoinType::RIGHT:
		return "RIGHT";
	case JoinType::INNER:
		return "INNER";
	case JoinType::OUTER:
		return "FULL";
	case JoinType::SEMI:
		return "SEMI";
	case JoinType::ANTI:
		return "ANTI";
	case JoinType::SINGLE:
		return "SINGLE";
	case JoinType::MARK:
		return "MARK";
	case JoinType::INVALID: // LCOV_EXCL_START
		break;
	}
	return "INVALID";
} // LCOV_EXCL_STOP

bool IsLeftOuterJoin(JoinType type) {
	return type == JoinType::LEFT || type == JoinType::OUTER;
}

bool IsRightOuterJoin(JoinType type) {
	return type == JoinType::OUTER || type == JoinType::RIGHT;
}

} // namespace duckdb


namespace duckdb {

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
string LogicalOperatorToString(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_GET:
		return "GET";
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return "CHUNK_GET";
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return "DELIM_GET";
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return "EMPTY_RESULT";
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return "EXPRESSION_GET";
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return "ANY_JOIN";
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return "COMPARISON_JOIN";
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return "DELIM_JOIN";
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return "PROJECTION";
	case LogicalOperatorType::LOGICAL_FILTER:
		return "FILTER";
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return "AGGREGATE";
	case LogicalOperatorType::LOGICAL_WINDOW:
		return "WINDOW";
	case LogicalOperatorType::LOGICAL_UNNEST:
		return "UNNEST";
	case LogicalOperatorType::LOGICAL_LIMIT:
		return "LIMIT";
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return "ORDER_BY";
	case LogicalOperatorType::LOGICAL_TOP_N:
		return "TOP_N";
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return "SAMPLE";
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
		return "LIMIT_PERCENT";
	case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
		return "COPY_TO_FILE";
	case LogicalOperatorType::LOGICAL_JOIN:
		return "JOIN";
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case LogicalOperatorType::LOGICAL_UNION:
		return "UNION";
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return "EXCEPT";
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return "INTERSECT";
	case LogicalOperatorType::LOGICAL_INSERT:
		return "INSERT";
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return "DISTINCT";
	case LogicalOperatorType::LOGICAL_DELETE:
		return "DELETE";
	case LogicalOperatorType::LOGICAL_UPDATE:
		return "UPDATE";
	case LogicalOperatorType::LOGICAL_PREPARE:
		return "PREPARE";
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return "DUMMY_SCAN";
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
		return "CREATE_INDEX";
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
		return "CREATE_TABLE";
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
		return "CREATE_MACRO";
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		return "EXPLAIN";
	case LogicalOperatorType::LOGICAL_EXECUTE:
		return "EXECUTE";
	case LogicalOperatorType::LOGICAL_VACUUM:
		return "VACUUM";
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		return "REC_CTE";
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return "CTE_SCAN";
	case LogicalOperatorType::LOGICAL_SHOW:
		return "SHOW";
	case LogicalOperatorType::LOGICAL_ALTER:
		return "ALTER";
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
		return "CREATE_SEQUENCE";
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:
		return "CREATE_TYPE";
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:
		return "CREATE_VIEW";
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
		return "CREATE_SCHEMA";
	case LogicalOperatorType::LOGICAL_DROP:
		return "DROP";
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return "PRAGMA";
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		return "TRANSACTION";
	case LogicalOperatorType::LOGICAL_EXPORT:
		return "EXPORT";
	case LogicalOperatorType::LOGICAL_SET:
		return "SET";
	case LogicalOperatorType::LOGICAL_LOAD:
		return "LOAD";
	case LogicalOperatorType::LOGICAL_INVALID:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb





namespace duckdb {

struct DefaultOptimizerType {
	const char *name;
	OptimizerType type;
};

static DefaultOptimizerType internal_optimizer_types[] = {
    {"expression_rewriter", OptimizerType::EXPRESSION_REWRITER},
    {"filter_pullup", OptimizerType::FILTER_PULLUP},
    {"filter_pushdown", OptimizerType::FILTER_PUSHDOWN},
    {"regex_range", OptimizerType::REGEX_RANGE},
    {"in_clause", OptimizerType::IN_CLAUSE},
    {"join_order", OptimizerType::JOIN_ORDER},
    {"deliminator", OptimizerType::DELIMINATOR},
    {"unused_columns", OptimizerType::UNUSED_COLUMNS},
    {"statistics_propagation", OptimizerType::STATISTICS_PROPAGATION},
    {"common_subexpressions", OptimizerType::COMMON_SUBEXPRESSIONS},
    {"common_aggregate", OptimizerType::COMMON_AGGREGATE},
    {"column_lifetime", OptimizerType::COLUMN_LIFETIME},
    {"top_n", OptimizerType::TOP_N},
    {"reorder_filter", OptimizerType::REORDER_FILTER},
    {nullptr, OptimizerType::INVALID}};

string OptimizerTypeToString(OptimizerType type) {
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		if (internal_optimizer_types[i].type == type) {
			return internal_optimizer_types[i].name;
		}
	}
	throw InternalException("Invalid optimizer type");
}

OptimizerType OptimizerTypeFromString(const string &str) {
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		if (internal_optimizer_types[i].name == str) {
			return internal_optimizer_types[i].type;
		}
	}
	// optimizer not found, construct candidate list
	vector<string> optimizer_names;
	for (idx_t i = 0; internal_optimizer_types[i].name; i++) {
		optimizer_names.emplace_back(internal_optimizer_types[i].name);
	}
	throw ParserException("Optimizer type \"%s\" not recognized\n%s", str,
	                      StringUtil::CandidatesErrorMessage(optimizer_names, str, "Candidate optimizers"));
}

} // namespace duckdb


namespace duckdb {

// LCOV_EXCL_START
string PhysicalOperatorToString(PhysicalOperatorType type) {
	switch (type) {
	case PhysicalOperatorType::TABLE_SCAN:
		return "TABLE_SCAN";
	case PhysicalOperatorType::DUMMY_SCAN:
		return "DUMMY_SCAN";
	case PhysicalOperatorType::CHUNK_SCAN:
		return "CHUNK_SCAN";
	case PhysicalOperatorType::DELIM_SCAN:
		return "DELIM_SCAN";
	case PhysicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case PhysicalOperatorType::LIMIT:
		return "LIMIT";
	case PhysicalOperatorType::LIMIT_PERCENT:
		return "LIMIT_PERCENT";
	case PhysicalOperatorType::STREAMING_LIMIT:
		return "STREAMING_LIMIT";
	case PhysicalOperatorType::RESERVOIR_SAMPLE:
		return "RESERVOIR_SAMPLE";
	case PhysicalOperatorType::STREAMING_SAMPLE:
		return "STREAMING_SAMPLE";
	case PhysicalOperatorType::TOP_N:
		return "TOP_N";
	case PhysicalOperatorType::WINDOW:
		return "WINDOW";
	case PhysicalOperatorType::STREAMING_WINDOW:
		return "STREAMING_WINDOW";
	case PhysicalOperatorType::UNNEST:
		return "UNNEST";
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
		return "SIMPLE_AGGREGATE";
	case PhysicalOperatorType::HASH_GROUP_BY:
		return "HASH_GROUP_BY";
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		return "PERFECT_HASH_GROUP_BY";
	case PhysicalOperatorType::FILTER:
		return "FILTER";
	case PhysicalOperatorType::PROJECTION:
		return "PROJECTION";
	case PhysicalOperatorType::COPY_TO_FILE:
		return "COPY_TO_FILE";
	case PhysicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		return "BLOCKWISE_NL_JOIN";
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
		return "NESTED_LOOP_JOIN";
	case PhysicalOperatorType::HASH_JOIN:
		return "HASH_JOIN";
	case PhysicalOperatorType::INDEX_JOIN:
		return "INDEX_JOIN";
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		return "PIECEWISE_MERGE_JOIN";
	case PhysicalOperatorType::IE_JOIN:
		return "IE_JOIN";
	case PhysicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case PhysicalOperatorType::UNION:
		return "UNION";
	case PhysicalOperatorType::INSERT:
		return "INSERT";
	case PhysicalOperatorType::DELETE_OPERATOR:
		return "DELETE";
	case PhysicalOperatorType::UPDATE:
		return "UPDATE";
	case PhysicalOperatorType::EMPTY_RESULT:
		return "EMPTY_RESULT";
	case PhysicalOperatorType::CREATE_TABLE:
		return "CREATE_TABLE";
	case PhysicalOperatorType::CREATE_TABLE_AS:
		return "CREATE_TABLE_AS";
	case PhysicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case PhysicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case PhysicalOperatorType::EXPLAIN_ANALYZE:
		return "EXPLAIN_ANALYZE";
	case PhysicalOperatorType::EXECUTE:
		return "EXECUTE";
	case PhysicalOperatorType::VACUUM:
		return "VACUUM";
	case PhysicalOperatorType::RECURSIVE_CTE:
		return "REC_CTE";
	case PhysicalOperatorType::RECURSIVE_CTE_SCAN:
		return "REC_CTE_SCAN";
	case PhysicalOperatorType::EXPRESSION_SCAN:
		return "EXPRESSION_SCAN";
	case PhysicalOperatorType::ALTER:
		return "ALTER";
	case PhysicalOperatorType::CREATE_SEQUENCE:
		return "CREATE_SEQUENCE";
	case PhysicalOperatorType::CREATE_VIEW:
		return "CREATE_VIEW";
	case PhysicalOperatorType::CREATE_SCHEMA:
		return "CREATE_SCHEMA";
	case PhysicalOperatorType::CREATE_MACRO:
		return "CREATE_MACRO";
	case PhysicalOperatorType::DROP:
		return "DROP";
	case PhysicalOperatorType::PRAGMA:
		return "PRAGMA";
	case PhysicalOperatorType::TRANSACTION:
		return "TRANSACTION";
	case PhysicalOperatorType::PREPARE:
		return "PREPARE";
	case PhysicalOperatorType::EXPORT:
		return "EXPORT";
	case PhysicalOperatorType::SET:
		return "SET";
	case PhysicalOperatorType::LOAD:
		return "LOAD";
	case PhysicalOperatorType::INOUT_FUNCTION:
		return "INOUT_FUNCTION";
	case PhysicalOperatorType::CREATE_TYPE:
		return "CREATE_TYPE";
	case PhysicalOperatorType::RESULT_COLLECTOR:
		return "RESULT_COLLECTOR";
	case PhysicalOperatorType::INVALID:
		break;
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

// LCOV_EXCL_START
string RelationTypeToString(RelationType type) {
	switch (type) {
	case RelationType::TABLE_RELATION:
		return "TABLE_RELATION";
	case RelationType::PROJECTION_RELATION:
		return "PROJECTION_RELATION";
	case RelationType::FILTER_RELATION:
		return "FILTER_RELATION";
	case RelationType::EXPLAIN_RELATION:
		return "EXPLAIN_RELATION";
	case RelationType::CROSS_PRODUCT_RELATION:
		return "CROSS_PRODUCT_RELATION";
	case RelationType::JOIN_RELATION:
		return "JOIN_RELATION";
	case RelationType::AGGREGATE_RELATION:
		return "AGGREGATE_RELATION";
	case RelationType::SET_OPERATION_RELATION:
		return "SET_OPERATION_RELATION";
	case RelationType::DISTINCT_RELATION:
		return "DISTINCT_RELATION";
	case RelationType::LIMIT_RELATION:
		return "LIMIT_RELATION";
	case RelationType::ORDER_RELATION:
		return "ORDER_RELATION";
	case RelationType::CREATE_VIEW_RELATION:
		return "CREATE_VIEW_RELATION";
	case RelationType::CREATE_TABLE_RELATION:
		return "CREATE_TABLE_RELATION";
	case RelationType::INSERT_RELATION:
		return "INSERT_RELATION";
	case RelationType::VALUE_LIST_RELATION:
		return "VALUE_LIST_RELATION";
	case RelationType::DELETE_RELATION:
		return "DELETE_RELATION";
	case RelationType::UPDATE_RELATION:
		return "UPDATE_RELATION";
	case RelationType::WRITE_CSV_RELATION:
		return "WRITE_CSV_RELATION";
	case RelationType::READ_CSV_RELATION:
		return "READ_CSV_RELATION";
	case RelationType::SUBQUERY_RELATION:
		return "SUBQUERY_RELATION";
	case RelationType::TABLE_FUNCTION_RELATION:
		return "TABLE_FUNCTION_RELATION";
	case RelationType::VIEW_RELATION:
		return "VIEW_RELATION";
	case RelationType::QUERY_RELATION:
		return "QUERY_RELATION";
	case RelationType::INVALID_RELATION:
		break;
	}
	return "INVALID_RELATION";
}
// LCOV_EXCL_STOP

} // namespace duckdb


namespace duckdb {

// LCOV_EXCL_START
string StatementTypeToString(StatementType type) {
	switch (type) {
	case StatementType::SELECT_STATEMENT:
		return "SELECT";
	case StatementType::INSERT_STATEMENT:
		return "INSERT";
	case StatementType::UPDATE_STATEMENT:
		return "UPDATE";
	case StatementType::DELETE_STATEMENT:
		return "DELETE";
	case StatementType::PREPARE_STATEMENT:
		return "PREPARE";
	case StatementType::EXECUTE_STATEMENT:
		return "EXECUTE";
	case StatementType::ALTER_STATEMENT:
		return "ALTER";
	case StatementType::TRANSACTION_STATEMENT:
		return "TRANSACTION";
	case StatementType::COPY_STATEMENT:
		return "COPY";
	case StatementType::ANALYZE_STATEMENT:
		return "ANALYZE";
	case StatementType::VARIABLE_SET_STATEMENT:
		return "VARIABLE_SET";
	case StatementType::CREATE_FUNC_STATEMENT:
		return "CREATE_FUNC";
	case StatementType::EXPLAIN_STATEMENT:
		return "EXPLAIN";
	case StatementType::CREATE_STATEMENT:
		return "CREATE";
	case StatementType::DROP_STATEMENT:
		return "DROP";
	case StatementType::PRAGMA_STATEMENT:
		return "PRAGMA";
	case StatementType::SHOW_STATEMENT:
		return "SHOW";
	case StatementType::VACUUM_STATEMENT:
		return "VACUUM";
	case StatementType::RELATION_STATEMENT:
		return "RELATION";
	case StatementType::EXPORT_STATEMENT:
		return "EXPORT";
	case StatementType::CALL_STATEMENT:
		return "CALL";
	case StatementType::SET_STATEMENT:
		return "SET";
	case StatementType::LOAD_STATEMENT:
		return "LOAD";
	case StatementType::EXTENSION_STATEMENT:
		return "EXTENSION";
	case StatementType::INVALID_STATEMENT:
		break;
	}
	return "INVALID";
}

string StatementReturnTypeToString(StatementReturnType type) {
	switch (type) {
	case StatementReturnType::QUERY_RESULT:
		return "QUERY_RESULT";
	case StatementReturnType::CHANGED_ROWS:
		return "CHANGED_ROWS";
	case StatementReturnType::NOTHING:
		return "NOTHING";
	}
	return "INVALID";
}
// LCOV_EXCL_STOP

} // namespace duckdb






namespace duckdb {

Exception::Exception(const string &msg) : std::exception(), type(ExceptionType::INVALID) {
	exception_message_ = msg;
}

Exception::Exception(ExceptionType exception_type, const string &message) : std::exception(), type(exception_type) {
	exception_message_ = ExceptionTypeToString(exception_type) + " Error: " + message;
}

const char *Exception::what() const noexcept {
	return exception_message_.c_str();
}

bool Exception::UncaughtException() {
#if __cplusplus >= 201703L
	return std::uncaught_exceptions() > 0;
#else
	return std::uncaught_exception();
#endif
}

string Exception::ConstructMessageRecursive(const string &msg, vector<ExceptionFormatValue> &values) {
	return ExceptionFormatValue::Format(msg, values);
}

string Exception::ExceptionTypeToString(ExceptionType type) {
	switch (type) {
	case ExceptionType::INVALID:
		return "Invalid";
	case ExceptionType::OUT_OF_RANGE:
		return "Out of Range";
	case ExceptionType::CONVERSION:
		return "Conversion";
	case ExceptionType::UNKNOWN_TYPE:
		return "Unknown Type";
	case ExceptionType::DECIMAL:
		return "Decimal";
	case ExceptionType::MISMATCH_TYPE:
		return "Mismatch Type";
	case ExceptionType::DIVIDE_BY_ZERO:
		return "Divide by Zero";
	case ExceptionType::OBJECT_SIZE:
		return "Object Size";
	case ExceptionType::INVALID_TYPE:
		return "Invalid type";
	case ExceptionType::SERIALIZATION:
		return "Serialization";
	case ExceptionType::TRANSACTION:
		return "TransactionContext";
	case ExceptionType::NOT_IMPLEMENTED:
		return "Not implemented";
	case ExceptionType::EXPRESSION:
		return "Expression";
	case ExceptionType::CATALOG:
		return "Catalog";
	case ExceptionType::PARSER:
		return "Parser";
	case ExceptionType::BINDER:
		return "Binder";
	case ExceptionType::PLANNER:
		return "Planner";
	case ExceptionType::SCHEDULER:
		return "Scheduler";
	case ExceptionType::EXECUTOR:
		return "Executor";
	case ExceptionType::CONSTRAINT:
		return "Constraint";
	case ExceptionType::INDEX:
		return "Index";
	case ExceptionType::STAT:
		return "Stat";
	case ExceptionType::CONNECTION:
		return "Connection";
	case ExceptionType::SYNTAX:
		return "Syntax";
	case ExceptionType::SETTINGS:
		return "Settings";
	case ExceptionType::OPTIMIZER:
		return "Optimizer";
	case ExceptionType::NULL_POINTER:
		return "NullPointer";
	case ExceptionType::IO:
		return "IO";
	case ExceptionType::INTERRUPT:
		return "INTERRUPT";
	case ExceptionType::FATAL:
		return "FATAL";
	case ExceptionType::INTERNAL:
		return "INTERNAL";
	case ExceptionType::INVALID_INPUT:
		return "Invalid Input";
	case ExceptionType::OUT_OF_MEMORY:
		return "Out of Memory";
	case ExceptionType::PERMISSION:
		return "Permission";
	default:
		return "Unknown";
	}
}

StandardException::StandardException(ExceptionType exception_type, const string &message)
    : Exception(exception_type, message) {
}

CastException::CastException(const PhysicalType orig_type, const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION,
                "Type " + TypeIdToString(orig_type) + " can't be cast as " + TypeIdToString(new_type)) {
}

CastException::CastException(const LogicalType &orig_type, const LogicalType &new_type)
    : Exception(ExceptionType::CONVERSION,
                "Type " + orig_type.ToString() + " can't be cast as " + new_type.ToString()) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const int64_t value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " +
                                               to_string((intmax_t)value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const double value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " + to_string(value) +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const hugeint_t value, const PhysicalType orig_type,
                                                   const PhysicalType new_type)
    : Exception(ExceptionType::CONVERSION, "Type " + TypeIdToString(orig_type) + " with value " + value.ToString() +
                                               " can't be cast because the value is out of range "
                                               "for the destination type " +
                                               TypeIdToString(new_type)) {
}

ValueOutOfRangeException::ValueOutOfRangeException(const PhysicalType var_type, const idx_t length)
    : Exception(ExceptionType::OUT_OF_RANGE,
                "The value is too long to fit into type " + TypeIdToString(var_type) + "(" + to_string(length) + ")") {
}

ConversionException::ConversionException(const string &msg) : Exception(ExceptionType::CONVERSION, msg) {
}

InvalidTypeException::InvalidTypeException(PhysicalType type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + TypeIdToString(type) + "]: " + msg) {
}

InvalidTypeException::InvalidTypeException(const LogicalType &type, const string &msg)
    : Exception(ExceptionType::INVALID_TYPE, "Invalid Type [" + type.ToString() + "]: " + msg) {
}

TypeMismatchException::TypeMismatchException(const PhysicalType type_1, const PhysicalType type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + TypeIdToString(type_1) + " does not match with " + TypeIdToString(type_2) + ". " + msg) {
}

TypeMismatchException::TypeMismatchException(const LogicalType &type_1, const LogicalType &type_2, const string &msg)
    : Exception(ExceptionType::MISMATCH_TYPE,
                "Type " + type_1.ToString() + " does not match with " + type_2.ToString() + ". " + msg) {
}

TransactionException::TransactionException(const string &msg) : Exception(ExceptionType::TRANSACTION, msg) {
}

NotImplementedException::NotImplementedException(const string &msg) : Exception(ExceptionType::NOT_IMPLEMENTED, msg) {
}

OutOfRangeException::OutOfRangeException(const string &msg) : Exception(ExceptionType::OUT_OF_RANGE, msg) {
}

CatalogException::CatalogException(const string &msg) : StandardException(ExceptionType::CATALOG, msg) {
}

ParserException::ParserException(const string &msg) : StandardException(ExceptionType::PARSER, msg) {
}

PermissionException::PermissionException(const string &msg) : StandardException(ExceptionType::PERMISSION, msg) {
}

SyntaxException::SyntaxException(const string &msg) : Exception(ExceptionType::SYNTAX, msg) {
}

ConstraintException::ConstraintException(const string &msg) : Exception(ExceptionType::CONSTRAINT, msg) {
}

BinderException::BinderException(const string &msg) : StandardException(ExceptionType::BINDER, msg) {
}

IOException::IOException(const string &msg) : Exception(ExceptionType::IO, msg) {
}

SerializationException::SerializationException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

SequenceException::SequenceException(const string &msg) : Exception(ExceptionType::SERIALIZATION, msg) {
}

InterruptException::InterruptException() : Exception(ExceptionType::INTERRUPT, "Interrupted!") {
}

FatalException::FatalException(const string &msg) : Exception(ExceptionType::FATAL, msg) {
}

InternalException::InternalException(const string &msg) : Exception(ExceptionType::INTERNAL, msg) {
}

InvalidInputException::InvalidInputException(const string &msg) : Exception(ExceptionType::INVALID_INPUT, msg) {
}

OutOfMemoryException::OutOfMemoryException(const string &msg) : Exception(ExceptionType::OUT_OF_MEMORY, msg) {
}

} // namespace duckdb






namespace duckdb {

ExceptionFormatValue::ExceptionFormatValue(double dbl_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE), dbl_val(dbl_val) {
}
ExceptionFormatValue::ExceptionFormatValue(int64_t int_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER), int_val(int_val) {
}
ExceptionFormatValue::ExceptionFormatValue(hugeint_t huge_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(Hugeint::ToString(huge_val)) {
}
ExceptionFormatValue::ExceptionFormatValue(string str_val)
    : type(ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING), str_val(move(str_val)) {
}

template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(PhysicalType value) {
	return ExceptionFormatValue(TypeIdToString(value));
}
template <>
ExceptionFormatValue
ExceptionFormatValue::CreateFormatValue(LogicalType value) { // NOLINT: templating requires us to copy value here
	return ExceptionFormatValue(value.ToString());
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(float value) {
	return ExceptionFormatValue(double(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(double value) {
	return ExceptionFormatValue(double(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(string value) {
	return ExceptionFormatValue(move(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(const char *value) {
	return ExceptionFormatValue(string(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(char *value) {
	return ExceptionFormatValue(string(value));
}
template <>
ExceptionFormatValue ExceptionFormatValue::CreateFormatValue(hugeint_t value) {
	return ExceptionFormatValue(value);
}

string ExceptionFormatValue::Format(const string &msg, vector<ExceptionFormatValue> &values) {
	std::vector<duckdb_fmt::basic_format_arg<duckdb_fmt::printf_context>> format_args;
	for (auto &val : values) {
		switch (val.type) {
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_DOUBLE:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.dbl_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_INTEGER:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.int_val));
			break;
		case ExceptionFormatValueType::FORMAT_VALUE_TYPE_STRING:
			format_args.push_back(duckdb_fmt::internal::make_arg<duckdb_fmt::printf_context>(val.str_val));
			break;
		}
	}
	return duckdb_fmt::vsprintf(msg, duckdb_fmt::basic_format_args<duckdb_fmt::printf_context>(
	                                     format_args.data(), static_cast<int>(format_args.size())));
}

} // namespace duckdb


namespace duckdb {

//===--------------------------------------------------------------------===//
// Field Writer
//===--------------------------------------------------------------------===//
FieldWriter::FieldWriter(Serializer &serializer_p)
    : serializer(serializer_p), buffer(make_unique<BufferedSerializer>()), field_count(0), finalized(false) {
}

FieldWriter::~FieldWriter() {
	if (Exception::UncaughtException()) {
		return;
	}
	D_ASSERT(finalized);
	// finalize should always have been called, unless this is destroyed as part of stack unwinding
	D_ASSERT(!buffer);
}

void FieldWriter::WriteData(const_data_ptr_t buffer_ptr, idx_t write_size) {
	D_ASSERT(buffer);
	buffer->WriteData(buffer_ptr, write_size);
}

template <>
void FieldWriter::Write(const string &val) {
	Write<uint32_t>((uint32_t)val.size());
	if (!val.empty()) {
		WriteData((const_data_ptr_t)val.c_str(), val.size());
	}
}

void FieldWriter::Finalize() {
	D_ASSERT(buffer);
	D_ASSERT(!finalized);
	finalized = true;
	serializer.Write<uint32_t>(field_count);
	serializer.Write<uint64_t>(buffer->blob.size);
	serializer.WriteData(buffer->blob.data.get(), buffer->blob.size);

	buffer.reset();
}

//===--------------------------------------------------------------------===//
// Field Deserializer
//===--------------------------------------------------------------------===//
FieldDeserializer::FieldDeserializer(Deserializer &root) : root(root), remaining_data(idx_t(-1)) {
}

void FieldDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
	D_ASSERT(remaining_data != idx_t(-1));
	D_ASSERT(read_size <= remaining_data);
	root.ReadData(buffer, read_size);
	remaining_data -= read_size;
}

idx_t FieldDeserializer::RemainingData() {
	return remaining_data;
}

void FieldDeserializer::SetRemainingData(idx_t remaining_data) {
	this->remaining_data = remaining_data;
}

//===--------------------------------------------------------------------===//
// Field Reader
//===--------------------------------------------------------------------===//
FieldReader::FieldReader(Deserializer &source_p) : source(source_p), field_count(0), finalized(false) {
	max_field_count = source_p.Read<uint32_t>();
	total_size = source_p.Read<uint64_t>();
	D_ASSERT(max_field_count > 0);
	source.SetRemainingData(total_size);
}

FieldReader::~FieldReader() {
	if (Exception::UncaughtException()) {
		return;
	}
	D_ASSERT(finalized);
}

void FieldReader::Finalize() {
	D_ASSERT(!finalized);
	finalized = true;
	if (field_count < max_field_count) {
		// we can handle this case by calling source.ReadData(buffer, source.RemainingData())
		throw SerializationException("Not all fields were read. This file might have been written with a newer version "
		                             "of DuckDB and is incompatible with this version of DuckDB.");
	}
	D_ASSERT(source.RemainingData() == 0);
}

} // namespace duckdb








#include <cstring>

namespace duckdb {

FileBuffer::FileBuffer(Allocator &allocator, FileBufferType type, uint64_t bufsiz)
    : allocator(allocator), type(type), malloced_buffer(nullptr) {
	SetMallocedSize(bufsiz);
	malloced_buffer = allocator.AllocateData(malloced_size);
	Construct(bufsiz);
}

FileBuffer::FileBuffer(FileBuffer &source, FileBufferType type_p) : allocator(source.allocator), type(type_p) {
	// take over the structures of the source buffer
	buffer = source.buffer;
	size = source.size;
	internal_buffer = source.internal_buffer;
	internal_size = source.internal_size;
	malloced_buffer = source.malloced_buffer;
	malloced_size = source.malloced_size;

	source.buffer = nullptr;
	source.size = 0;
	source.internal_buffer = nullptr;
	source.internal_size = 0;
	source.malloced_buffer = nullptr;
	source.malloced_size = 0;
}

FileBuffer::~FileBuffer() {
	if (!malloced_buffer) {
		return;
	}
	allocator.FreeData(malloced_buffer, malloced_size);
}

void FileBuffer::SetMallocedSize(uint64_t &bufsiz) {
	// make room for the block header (if this is not the db file header)
	if (type == FileBufferType::MANAGED_BUFFER && bufsiz != Storage::FILE_HEADER_SIZE) {
		bufsiz += Storage::BLOCK_HEADER_SIZE;
	}
	malloced_size = bufsiz;
}

void FileBuffer::Construct(uint64_t bufsiz) {
	if (!malloced_buffer) {
		throw std::bad_alloc();
	}
	internal_buffer = malloced_buffer;
	internal_size = malloced_size;
	buffer = internal_buffer + Storage::BLOCK_HEADER_SIZE;
	size = internal_size - Storage::BLOCK_HEADER_SIZE;
}

void FileBuffer::Resize(uint64_t bufsiz) {
	D_ASSERT(type == FileBufferType::MANAGED_BUFFER);
	auto old_size = malloced_size;
	SetMallocedSize(bufsiz);
	malloced_buffer = allocator.ReallocateData(malloced_buffer, old_size, malloced_size);
	Construct(bufsiz);
}

void FileBuffer::Read(FileHandle &handle, uint64_t location) {
	handle.Read(internal_buffer, internal_size, location);
}

void FileBuffer::ReadAndChecksum(FileHandle &handle, uint64_t location) {
	// read the buffer from disk
	Read(handle, location);
	// compute the checksum
	auto stored_checksum = Load<uint64_t>(internal_buffer);
	uint64_t computed_checksum = Checksum(buffer, size);
	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block",
		                  computed_checksum, stored_checksum);
	}
}

void FileBuffer::Write(FileHandle &handle, uint64_t location) {
	handle.Write(internal_buffer, internal_size, location);
}

void FileBuffer::ChecksumAndWrite(FileHandle &handle, uint64_t location) {
	// compute the checksum and write it to the start of the buffer (if not temp buffer)
	uint64_t checksum = Checksum(buffer, size);
	Store<uint64_t>(checksum, internal_buffer);
	// now write the buffer
	Write(handle, location);
}

void FileBuffer::Clear() {
	memset(internal_buffer, 0, internal_size);
}

} // namespace duckdb












#include <cstdint>
#include <cstdio>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#else
#include <string>
#include <sysinfoapi.h>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

namespace duckdb {

FileSystem::~FileSystem() {
}

FileSystem &FileSystem::GetFileSystem(ClientContext &context) {
	return *context.db->config.file_system;
}

FileOpener *FileSystem::GetFileOpener(ClientContext &context) {
	return ClientData::Get(context).file_opener.get();
}

#ifndef _WIN32
string FileSystem::PathSeparator() {
	return "/";
}

void FileSystem::SetWorkingDirectory(const string &path) {
	if (chdir(path.c_str()) != 0) {
		throw IOException("Could not change working directory!");
	}
}

idx_t FileSystem::GetAvailableMemory() {
	errno = 0;
	idx_t max_memory = MinValue<idx_t>((idx_t)sysconf(_SC_PHYS_PAGES) * (idx_t)sysconf(_SC_PAGESIZE), UINTPTR_MAX);
	if (errno != 0) {
		return DConstants::INVALID_INDEX;
	}
	return max_memory;
}

string FileSystem::GetWorkingDirectory() {
	auto buffer = unique_ptr<char[]>(new char[PATH_MAX]);
	char *ret = getcwd(buffer.get(), PATH_MAX);
	if (!ret) {
		throw IOException("Could not get working directory!");
	}
	return string(buffer.get());
}
#else

string FileSystem::PathSeparator() {
	return "\\";
}

void FileSystem::SetWorkingDirectory(const string &path) {
	if (!SetCurrentDirectory(path.c_str())) {
		throw IOException("Could not change working directory!");
	}
}

idx_t FileSystem::GetAvailableMemory() {
	ULONGLONG available_memory_kb;
	if (GetPhysicallyInstalledSystemMemory(&available_memory_kb)) {
		return MinValue<idx_t>(available_memory_kb * 1000, UINTPTR_MAX);
	}
	// fallback: try GlobalMemoryStatusEx
	MEMORYSTATUSEX mem_state;
	mem_state.dwLength = sizeof(MEMORYSTATUSEX);

	if (GlobalMemoryStatusEx(&mem_state)) {
		return MinValue<idx_t>(mem_state.ullTotalPhys, UINTPTR_MAX);
	}
	return DConstants::INVALID_INDEX;
}

string FileSystem::GetWorkingDirectory() {
	idx_t count = GetCurrentDirectory(0, nullptr);
	if (count == 0) {
		throw IOException("Could not get working directory!");
	}
	auto buffer = unique_ptr<char[]>(new char[count]);
	idx_t ret = GetCurrentDirectory(count, buffer.get());
	if (count != ret + 1) {
		throw IOException("Could not get working directory!");
	}
	return string(buffer.get(), ret);
}

#endif

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}

string FileSystem::ConvertSeparators(const string &path) {
	auto separator_str = PathSeparator();
	char separator = separator_str[0];
	if (separator == '/') {
		// on unix-based systems we only accept / as a separator
		return path;
	}
	// on windows-based systems we accept both
	string result = path;
	for (idx_t i = 0; i < result.size(); i++) {
		if (result[i] == '/') {
			result[i] = separator;
		}
	}
	return result;
}

string FileSystem::ExtractBaseName(const string &path) {
	auto normalized_path = ConvertSeparators(path);
	auto sep = PathSeparator();
	auto vec = StringUtil::Split(StringUtil::Split(normalized_path, sep).back(), ".");
	return vec[0];
}

string FileSystem::GetHomeDirectory() {
#ifdef DUCKDB_WINDOWS
	const char *homedir = getenv("USERPROFILE");
#else
	const char *homedir = getenv("HOME");
#endif
	if (homedir) {
		return homedir;
	}
	return string();
}

// LCOV_EXCL_START
unique_ptr<FileHandle> FileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                            FileCompressionType compression, FileOpener *opener) {
	throw NotImplementedException("%s: OpenFile is not implemented!", GetName());
}

void FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Read (with location) is not implemented!", GetName());
}

void FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Write (with location) is not implemented!", GetName());
}

int64_t FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Read is not implemented!", GetName());
}

int64_t FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Write is not implemented!", GetName());
}

int64_t FileSystem::GetFileSize(FileHandle &handle) {
	throw NotImplementedException("%s: GetFileSize is not implemented!", GetName());
}

time_t FileSystem::GetLastModifiedTime(FileHandle &handle) {
	throw NotImplementedException("%s: GetLastModifiedTime is not implemented!", GetName());
}

FileType FileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_INVALID;
}

void FileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	throw NotImplementedException("%s: Truncate is not implemented!", GetName());
}

bool FileSystem::DirectoryExists(const string &directory) {
	throw NotImplementedException("%s: DirectoryExists is not implemented!", GetName());
}

void FileSystem::CreateDirectory(const string &directory) {
	throw NotImplementedException("%s: CreateDirectory is not implemented!", GetName());
}

void FileSystem::RemoveDirectory(const string &directory) {
	throw NotImplementedException("%s: RemoveDirectory is not implemented!", GetName());
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	throw NotImplementedException("%s: ListFiles is not implemented!", GetName());
}

void FileSystem::MoveFile(const string &source, const string &target) {
	throw NotImplementedException("%s: MoveFile is not implemented!", GetName());
}

bool FileSystem::FileExists(const string &filename) {
	throw NotImplementedException("%s: FileExists is not implemented!", GetName());
}

bool FileSystem::IsPipe(const string &filename) {
	throw NotImplementedException("%s: IsPipe is not implemented!", GetName());
}

void FileSystem::RemoveFile(const string &filename) {
	throw NotImplementedException("%s: RemoveFile is not implemented!", GetName());
}

void FileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("%s: FileSync is not implemented!", GetName());
}

vector<string> FileSystem::Glob(const string &path, FileOpener *opener) {
	throw NotImplementedException("%s: Glob is not implemented!", GetName());
}

vector<string> FileSystem::Glob(const string &path, ClientContext &context) {
	return Glob(path, GetFileOpener(context));
}

void FileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

void FileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

bool FileSystem::CanHandleFile(const string &fpath) {
	throw NotImplementedException("%s: CanHandleFile is not implemented!", GetName());
}

void FileSystem::Seek(FileHandle &handle, idx_t location) {
	throw NotImplementedException("%s: Seek is not implemented!", GetName());
}

void FileSystem::Reset(FileHandle &handle) {
	handle.Seek(0);
}

idx_t FileSystem::SeekPosition(FileHandle &handle) {
	throw NotImplementedException("%s: SeekPosition is not implemented!", GetName());
}

bool FileSystem::CanSeek() {
	throw NotImplementedException("%s: CanSeek is not implemented!", GetName());
}

unique_ptr<FileHandle> FileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) {
	throw NotImplementedException("%s: OpenCompressedFile is not implemented!", GetName());
}

bool FileSystem::OnDiskFile(FileHandle &handle) {
	throw NotImplementedException("%s: OnDiskFile is not implemented!", GetName());
}
// LCOV_EXCL_STOP

FileHandle::FileHandle(FileSystem &file_system, string path_p) : file_system(file_system), path(move(path_p)) {
}

FileHandle::~FileHandle() {
}

int64_t FileHandle::Read(void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, nr_bytes);
}

int64_t FileHandle::Write(void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, nr_bytes);
}

void FileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

void FileHandle::Seek(idx_t location) {
	file_system.Seek(*this, location);
}

void FileHandle::Reset() {
	file_system.Reset(*this);
}

idx_t FileHandle::SeekPosition() {
	return file_system.SeekPosition(*this);
}

bool FileHandle::CanSeek() {
	return file_system.CanSeek();
}

string FileHandle::ReadLine() {
	string result;
	char buffer[1];
	while (true) {
		idx_t tuples_read = Read(buffer, 1);
		if (tuples_read == 0 || buffer[0] == '\n') {
			return result;
		}
		if (buffer[0] != '\r') {
			result += buffer[0];
		}
	}
}

bool FileHandle::OnDiskFile() {
	return file_system.OnDiskFile(*this);
}

idx_t FileHandle::GetFileSize() {
	return file_system.GetFileSize(*this);
}

void FileHandle::Sync() {
	file_system.FileSync(*this);
}

void FileHandle::Truncate(int64_t new_size) {
	file_system.Truncate(*this, new_size);
}

FileType FileHandle::GetType() {
	return file_system.GetFileType(*this);
}

} // namespace duckdb









namespace duckdb {

/*

  0      2 bytes  magic header  0x1f, 0x8b (\037 \213)
  2      1 byte   compression method
                     0: store (copied)
                     1: compress
                     2: pack
                     3: lzh
                     4..7: reserved
                     8: deflate
  3      1 byte   flags
                     bit 0 set: file probably ascii text
                     bit 1 set: continuation of multi-part gzip file, part number present
                     bit 2 set: extra field present
                     bit 3 set: original file name present
                     bit 4 set: file comment present
                     bit 5 set: file is encrypted, encryption header present
                     bit 6,7:   reserved
  4      4 bytes  file modification time in Unix format
  8      1 byte   extra flags (depend on compression method)
  9      1 byte   OS type
[
         2 bytes  optional part number (second part=1)
]?
[
         2 bytes  optional extra field length (e)
        (e)bytes  optional extra field
]?
[
           bytes  optional original file name, zero terminated
]?
[
           bytes  optional file comment, zero terminated
]?
[
        12 bytes  optional encryption header
]?
           bytes  compressed data
         4 bytes  crc32
         4 bytes  uncompressed input size modulo 2^32

 */

static idx_t GZipConsumeString(FileHandle &input) {
	idx_t size = 1; // terminator
	char buffer[1];
	while (input.Read(buffer, 1) == 1) {
		if (buffer[0] == '\0') {
			break;
		}
		size++;
	}
	return size;
}

struct MiniZStreamWrapper : public StreamWrapper {
	~MiniZStreamWrapper() override;

	CompressedFile *file = nullptr;
	duckdb_miniz::mz_stream *mz_stream_ptr = nullptr;
	bool writing = false;
	duckdb_miniz::mz_ulong crc;
	idx_t total_size;

public:
	void Initialize(CompressedFile &file, bool write) override;

	bool Read(StreamData &stream_data) override;
	void Write(CompressedFile &file, StreamData &stream_data, data_ptr_t buffer, int64_t nr_bytes) override;

	void Close() override;

	void FlushStream();
};

MiniZStreamWrapper::~MiniZStreamWrapper() {
	// avoid closing if destroyed during stack unwinding
	if (Exception::UncaughtException()) {
		return;
	}
	try {
		Close();
	} catch (...) {
	}
}

void MiniZStreamWrapper::Initialize(CompressedFile &file, bool write) {
	Close();
	this->file = &file;
	mz_stream_ptr = new duckdb_miniz::mz_stream();
	memset(mz_stream_ptr, 0, sizeof(duckdb_miniz::mz_stream));
	this->writing = write;

	// TODO use custom alloc/free methods in miniz to throw exceptions on OOM
	uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];
	if (write) {
		crc = MZ_CRC32_INIT;
		total_size = 0;

		MiniZStream::InitializeGZIPHeader(gzip_hdr);
		file.child_handle->Write(gzip_hdr, GZIP_HEADER_MINSIZE);

		auto ret = mz_deflateInit2((duckdb_miniz::mz_streamp)mz_stream_ptr, duckdb_miniz::MZ_DEFAULT_LEVEL, MZ_DEFLATED,
		                           -MZ_DEFAULT_WINDOW_BITS, 1, 0);
		if (ret != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to initialize miniz");
		}
	} else {
		idx_t data_start = GZIP_HEADER_MINSIZE;
		auto read_count = file.child_handle->Read(gzip_hdr, GZIP_HEADER_MINSIZE);
		GZipFileSystem::VerifyGZIPHeader(gzip_hdr, read_count);

		if (gzip_hdr[3] & GZIP_FLAG_NAME) {
			file.child_handle->Seek(data_start);
			data_start += GZipConsumeString(*file.child_handle);
		}
		file.child_handle->Seek(data_start);
		// stream is now set to beginning of payload data
		auto ret = duckdb_miniz::mz_inflateInit2((duckdb_miniz::mz_streamp)mz_stream_ptr, -MZ_DEFAULT_WINDOW_BITS);
		if (ret != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to initialize miniz");
		}
	}
}

bool MiniZStreamWrapper::Read(StreamData &sd) {
	// actually decompress
	mz_stream_ptr->next_in = (data_ptr_t)sd.in_buff_start;
	D_ASSERT(sd.in_buff_end - sd.in_buff_start < NumericLimits<int32_t>::Maximum());
	mz_stream_ptr->avail_in = (uint32_t)(sd.in_buff_end - sd.in_buff_start);
	mz_stream_ptr->next_out = (data_ptr_t)sd.out_buff_end;
	mz_stream_ptr->avail_out = (uint32_t)((sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_end);
	auto ret = duckdb_miniz::mz_inflate(mz_stream_ptr, duckdb_miniz::MZ_NO_FLUSH);
	if (ret != duckdb_miniz::MZ_OK && ret != duckdb_miniz::MZ_STREAM_END) {
		throw IOException("Failed to decode gzip stream: %s", duckdb_miniz::mz_error(ret));
	}
	// update pointers following inflate()
	sd.in_buff_start = (data_ptr_t)mz_stream_ptr->next_in;
	sd.in_buff_end = sd.in_buff_start + mz_stream_ptr->avail_in;
	sd.out_buff_end = (data_ptr_t)mz_stream_ptr->next_out;
	D_ASSERT(sd.out_buff_end + mz_stream_ptr->avail_out == sd.out_buff.get() + sd.out_buf_size);
	// if stream ended, deallocate inflator
	if (ret == duckdb_miniz::MZ_STREAM_END) {
		Close();
		return true;
	}
	return false;
}

void MiniZStreamWrapper::Write(CompressedFile &file, StreamData &sd, data_ptr_t uncompressed_data,
                               int64_t uncompressed_size) {
	// update the src and the total size
	crc = duckdb_miniz::mz_crc32(crc, (const unsigned char *)uncompressed_data, uncompressed_size);
	total_size += uncompressed_size;

	auto remaining = uncompressed_size;
	while (remaining > 0) {
		idx_t output_remaining = (sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start;

		mz_stream_ptr->next_in = (const unsigned char *)uncompressed_data;
		mz_stream_ptr->avail_in = remaining;
		mz_stream_ptr->next_out = sd.out_buff_start;
		mz_stream_ptr->avail_out = output_remaining;

		auto res = mz_deflate(mz_stream_ptr, duckdb_miniz::MZ_NO_FLUSH);
		if (res != duckdb_miniz::MZ_OK) {
			D_ASSERT(res != duckdb_miniz::MZ_STREAM_END);
			throw InternalException("Failed to compress GZIP block");
		}
		sd.out_buff_start += output_remaining - mz_stream_ptr->avail_out;
		if (mz_stream_ptr->avail_out == 0) {
			// no more output buffer available: flush
			file.child_handle->Write(sd.out_buff.get(), sd.out_buff_start - sd.out_buff.get());
			sd.out_buff_start = sd.out_buff.get();
		}
		idx_t written = remaining - mz_stream_ptr->avail_in;
		uncompressed_data += written;
		remaining = mz_stream_ptr->avail_in;
	}
}

void MiniZStreamWrapper::FlushStream() {
	auto &sd = file->stream_data;
	mz_stream_ptr->next_in = nullptr;
	mz_stream_ptr->avail_in = 0;
	while (true) {
		auto output_remaining = (sd.out_buff.get() + sd.out_buf_size) - sd.out_buff_start;
		mz_stream_ptr->next_out = sd.out_buff_start;
		mz_stream_ptr->avail_out = output_remaining;

		auto res = mz_deflate(mz_stream_ptr, duckdb_miniz::MZ_FINISH);
		sd.out_buff_start += (output_remaining - mz_stream_ptr->avail_out);
		if (sd.out_buff_start > sd.out_buff.get()) {
			file->child_handle->Write(sd.out_buff.get(), sd.out_buff_start - sd.out_buff.get());
			sd.out_buff_start = sd.out_buff.get();
		}
		if (res == duckdb_miniz::MZ_STREAM_END) {
			break;
		}
		if (res != duckdb_miniz::MZ_OK) {
			throw InternalException("Failed to compress GZIP block");
		}
	}
}

void MiniZStreamWrapper::Close() {
	if (!mz_stream_ptr) {
		return;
	}
	if (writing) {
		// flush anything remaining in the stream
		FlushStream();

		// write the footer
		unsigned char gzip_footer[MiniZStream::GZIP_FOOTER_SIZE];
		MiniZStream::InitializeGZIPFooter(gzip_footer, crc, total_size);
		file->child_handle->Write(gzip_footer, MiniZStream::GZIP_FOOTER_SIZE);

		duckdb_miniz::mz_deflateEnd(mz_stream_ptr);
	} else {
		duckdb_miniz::mz_inflateEnd(mz_stream_ptr);
	}
	delete mz_stream_ptr;
	mz_stream_ptr = nullptr;
	file = nullptr;
}

class GZipFile : public CompressedFile {
public:
	GZipFile(unique_ptr<FileHandle> child_handle_p, const string &path, bool write)
	    : CompressedFile(gzip_fs, move(child_handle_p), path) {
		Initialize(write);
	}

	GZipFileSystem gzip_fs;
};

void GZipFileSystem::VerifyGZIPHeader(uint8_t gzip_hdr[], idx_t read_count) {
	// check for incorrectly formatted files
	if (read_count != GZIP_HEADER_MINSIZE) {
		throw IOException("Input is not a GZIP stream");
	}
	if (gzip_hdr[0] != 0x1F || gzip_hdr[1] != 0x8B) { // magic header
		throw IOException("Input is not a GZIP stream");
	}
	if (gzip_hdr[2] != GZIP_COMPRESSION_DEFLATE) { // compression method
		throw IOException("Unsupported GZIP compression method");
	}
	if (gzip_hdr[3] & GZIP_FLAG_UNSUPPORTED) {
		throw IOException("Unsupported GZIP archive");
	}
}

string GZipFileSystem::UncompressGZIPString(const string &in) {
	// decompress file
	auto body_ptr = in.data();

	auto mz_stream_ptr = new duckdb_miniz::mz_stream();
	memset(mz_stream_ptr, 0, sizeof(duckdb_miniz::mz_stream));

	uint8_t gzip_hdr[GZIP_HEADER_MINSIZE];

	// check for incorrectly formatted files

	// TODO this is mostly the same as gzip_file_system.cpp
	if (in.size() < GZIP_HEADER_MINSIZE) {
		throw IOException("Input is not a GZIP stream");
	}
	memcpy(gzip_hdr, body_ptr, GZIP_HEADER_MINSIZE);
	body_ptr += GZIP_HEADER_MINSIZE;
	GZipFileSystem::VerifyGZIPHeader(gzip_hdr, GZIP_HEADER_MINSIZE);

	if (gzip_hdr[3] & GZIP_FLAG_NAME) {
		char c;
		do {
			c = *body_ptr;
			body_ptr++;
		} while (c != '\0' && (idx_t)(body_ptr - in.data()) < in.size());
	}

	// stream is now set to beginning of payload data
	auto status = duckdb_miniz::mz_inflateInit2(mz_stream_ptr, -MZ_DEFAULT_WINDOW_BITS);
	if (status != duckdb_miniz::MZ_OK) {
		throw InternalException("Failed to initialize miniz");
	}

	auto bytes_remaining = in.size() - (body_ptr - in.data());
	mz_stream_ptr->next_in = (unsigned char *)body_ptr;
	mz_stream_ptr->avail_in = bytes_remaining;

	unsigned char decompress_buffer[BUFSIZ];
	string decompressed;

	while (status == duckdb_miniz::MZ_OK) {
		mz_stream_ptr->next_out = decompress_buffer;
		mz_stream_ptr->avail_out = sizeof(decompress_buffer);
		status = mz_inflate(mz_stream_ptr, duckdb_miniz::MZ_NO_FLUSH);
		if (status != duckdb_miniz::MZ_STREAM_END && status != duckdb_miniz::MZ_OK) {
			throw IOException("Failed to uncompress");
		}
		decompressed.append((char *)decompress_buffer, mz_stream_ptr->total_out - decompressed.size());
	}
	duckdb_miniz::mz_inflateEnd(mz_stream_ptr);
	if (decompressed.empty()) {
		throw IOException("Failed to uncompress");
	}
	return decompressed;
}

unique_ptr<FileHandle> GZipFileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) {
	auto path = handle->path;
	return make_unique<GZipFile>(move(handle), path, write);
}

unique_ptr<StreamWrapper> GZipFileSystem::CreateStream() {
	return make_unique<MiniZStreamWrapper>();
}

idx_t GZipFileSystem::InBufferSize() {
	return BUFFER_SIZE;
}

idx_t GZipFileSystem::OutBufferSize() {
	return BUFFER_SIZE;
}

} // namespace duckdb




#include <limits>

namespace duckdb {

using std::numeric_limits;

int8_t NumericLimits<int8_t>::Minimum() {
	return numeric_limits<int8_t>::lowest();
}

int8_t NumericLimits<int8_t>::Maximum() {
	return numeric_limits<int8_t>::max();
}

int16_t NumericLimits<int16_t>::Minimum() {
	return numeric_limits<int16_t>::lowest();
}

int16_t NumericLimits<int16_t>::Maximum() {
	return numeric_limits<int16_t>::max();
}

int32_t NumericLimits<int32_t>::Minimum() {
	return numeric_limits<int32_t>::lowest();
}

int32_t NumericLimits<int32_t>::Maximum() {
	return numeric_limits<int32_t>::max();
}

int64_t NumericLimits<int64_t>::Minimum() {
	return numeric_limits<int64_t>::lowest();
}

int64_t NumericLimits<int64_t>::Maximum() {
	return numeric_limits<int64_t>::max();
}

float NumericLimits<float>::Minimum() {
	return numeric_limits<float>::lowest();
}

float NumericLimits<float>::Maximum() {
	return numeric_limits<float>::max();
}

double NumericLimits<double>::Minimum() {
	return numeric_limits<double>::lowest();
}

double NumericLimits<double>::Maximum() {
	return numeric_limits<double>::max();
}

uint8_t NumericLimits<uint8_t>::Minimum() {
	return numeric_limits<uint8_t>::lowest();
}

uint8_t NumericLimits<uint8_t>::Maximum() {
	return numeric_limits<uint8_t>::max();
}

uint16_t NumericLimits<uint16_t>::Minimum() {
	return numeric_limits<uint16_t>::lowest();
}

uint16_t NumericLimits<uint16_t>::Maximum() {
	return numeric_limits<uint16_t>::max();
}

uint32_t NumericLimits<uint32_t>::Minimum() {
	return numeric_limits<uint32_t>::lowest();
}

uint32_t NumericLimits<uint32_t>::Maximum() {
	return numeric_limits<uint32_t>::max();
}

uint64_t NumericLimits<uint64_t>::Minimum() {
	return numeric_limits<uint64_t>::lowest();
}

uint64_t NumericLimits<uint64_t>::Maximum() {
	return numeric_limits<uint64_t>::max();
}

hugeint_t NumericLimits<hugeint_t>::Minimum() {
	hugeint_t result;
	result.lower = 1;
	result.upper = numeric_limits<int64_t>::lowest();
	return result;
}

hugeint_t NumericLimits<hugeint_t>::Maximum() {
	hugeint_t result;
	result.lower = numeric_limits<uint64_t>::max();
	result.upper = numeric_limits<int64_t>::max();
	return result;
}

} // namespace duckdb












#include <cstdint>
#include <cstdio>
#include <sys/stat.h>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#else


#include <io.h>
#include <string>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

namespace duckdb {

static void AssertValidFileFlags(uint8_t flags) {
#ifdef DEBUG
	bool is_read = flags & FileFlags::FILE_FLAGS_READ;
	bool is_write = flags & FileFlags::FILE_FLAGS_WRITE;
	// require either READ or WRITE (or both)
	D_ASSERT(is_read || is_write);
	// CREATE/Append flags require writing
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_APPEND));
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_FILE_CREATE));
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
	// cannot combine CREATE and CREATE_NEW flags
	D_ASSERT(!(flags & FileFlags::FILE_FLAGS_FILE_CREATE && flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
#endif
}

#ifndef _WIN32
bool LocalFileSystem::FileExists(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (S_ISREG(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

bool LocalFileSystem::IsPipe(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (S_ISFIFO(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

#else
bool LocalFileSystem::FileExists(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stati64 status;
		_wstati64(wpath, &status);
		if (status.st_mode & S_IFREG) {
			return true;
		}
	}
	return false;
}
bool LocalFileSystem::IsPipe(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stati64 status;
		_wstati64(wpath, &status);
		if (status.st_mode & _S_IFCHR) {
			return true;
		}
	}
	return false;
}
#endif

#ifndef _WIN32
// somehow sometimes this is missing
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

// Solaris
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

struct UnixFileHandle : public FileHandle {
public:
	UnixFileHandle(FileSystem &file_system, string path, int fd) : FileHandle(file_system, move(path)), fd(fd) {
	}
	~UnixFileHandle() override {
		Close();
	}

	int fd;

public:
	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};
};

static FileType GetFileTypeInternal(int fd) { // LCOV_EXCL_START
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return FileType::FILE_TYPE_INVALID;
	}
	switch (s.st_mode & S_IFMT) {
	case S_IFBLK:
		return FileType::FILE_TYPE_BLOCKDEV;
	case S_IFCHR:
		return FileType::FILE_TYPE_CHARDEV;
	case S_IFIFO:
		return FileType::FILE_TYPE_FIFO;
	case S_IFDIR:
		return FileType::FILE_TYPE_DIR;
	case S_IFLNK:
		return FileType::FILE_TYPE_LINK;
	case S_IFREG:
		return FileType::FILE_TYPE_REGULAR;
	case S_IFSOCK:
		return FileType::FILE_TYPE_SOCKET;
	default:
		return FileType::FILE_TYPE_INVALID;
	}
} // LCOV_EXCL_STOP

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock_type,
                                                 FileCompressionType compression, FileOpener *opener) {
	if (compression != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}

	AssertValidFileFlags(flags);

	int open_flags = 0;
	int rc;
	bool open_read = flags & FileFlags::FILE_FLAGS_READ;
	bool open_write = flags & FileFlags::FILE_FLAGS_WRITE;
	if (open_read && open_write) {
		open_flags = O_RDWR;
	} else if (open_read) {
		open_flags = O_RDONLY;
	} else if (open_write) {
		open_flags = O_WRONLY;
	} else {
		throw InternalException("READ, WRITE or both should be specified when opening a file");
	}
	if (open_write) {
		// need Read or Write
		D_ASSERT(flags & FileFlags::FILE_FLAGS_WRITE);
		open_flags |= O_CLOEXEC;
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			open_flags |= O_CREAT;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			open_flags |= O_CREAT | O_TRUNC;
		}
		if (flags & FileFlags::FILE_FLAGS_APPEND) {
			open_flags |= O_APPEND;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
#if defined(__sun) && defined(__SVR4)
		throw Exception("DIRECT_IO not supported on Solaris");
#endif
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
		// OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
		open_flags |= O_SYNC;
#else
		open_flags |= O_DIRECT | O_SYNC;
#endif
	}
	int fd = open(path.c_str(), open_flags, 0666);
	if (fd == -1) {
		throw IOException("Cannot open file \"%s\": %s", path, strerror(errno));
	}
	// #if defined(__DARWIN__) || defined(__APPLE__)
	// 	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
	// 		// OSX requires fcntl for Direct IO
	// 		rc = fcntl(fd, F_NOCACHE, 1);
	// 		if (fd == -1) {
	// 			throw IOException("Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
	// 		}
	// 	}
	// #endif
	if (lock_type != FileLockType::NO_LOCK) {
		// set lock on file
		// but only if it is not an input/output stream
		auto file_type = GetFileTypeInternal(fd);
		if (file_type != FileType::FILE_TYPE_FIFO && file_type != FileType::FILE_TYPE_SOCKET) {
			struct flock fl;
			memset(&fl, 0, sizeof fl);
			fl.l_type = lock_type == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = 0;
			rc = fcntl(fd, F_SETLK, &fl);
			if (rc == -1) {
				throw IOException("Could not set lock on file \"%s\": %s", path, strerror(errno));
			}
		}
	}
	return make_unique<UnixFileHandle>(*this, path, fd);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t offset = lseek(fd, location, SEEK_SET);
	if (offset == (off_t)-1) {
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path,
		                  strerror(errno));
	}
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t position = lseek(fd, 0, SEEK_CUR);
	if (position == (off_t)-1) {
		throw IOException("Could not get file position file \"%s\": %s", handle.path, strerror(errno));
	}
	return position;
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_read = pread(fd, buffer, nr_bytes, location);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
	}
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read all bytes from file \"%s\": wanted=%lld read=%lld", handle.path, nr_bytes,
		                  bytes_read);
	}
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_read = read(fd, buffer, nr_bytes);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_read;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_written = pwrite(fd, buffer, nr_bytes, location);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path, strerror(errno));
	}
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write all bytes to file \"%s\": wanted=%lld wrote=%lld", handle.path, nr_bytes,
		                  bytes_written);
	}
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_written = write(fd, buffer, nr_bytes);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_written;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return -1;
	}
	return s.st_size;
}

time_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return -1;
	}
	return s.st_mtime;
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	return GetFileTypeInternal(fd);
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (ftruncate(fd, new_size) != 0) {
		throw IOException("Could not truncate file \"%s\": %s", handle.path, strerror(errno));
	}
}

bool LocalFileSystem::DirectoryExists(const string &directory) {
	if (!directory.empty()) {
		if (access(directory.c_str(), 0) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

void LocalFileSystem::CreateDirectory(const string &directory) {
	struct stat st;

	if (stat(directory.c_str(), &st) != 0) {
		/* Directory does not exist. EEXIST for race condition */
		if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
			throw IOException("Failed to create directory \"%s\"!", directory);
		}
	} else if (!S_ISDIR(st.st_mode)) {
		throw IOException("Failed to create directory \"%s\": path exists but is not a directory!", directory);
	}
}

int RemoveDirectoryRecursive(const char *path) {
	DIR *d = opendir(path);
	idx_t path_len = (idx_t)strlen(path);
	int r = -1;

	if (d) {
		struct dirent *p;
		r = 0;
		while (!r && (p = readdir(d))) {
			int r2 = -1;
			char *buf;
			idx_t len;
			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}
			len = path_len + (idx_t)strlen(p->d_name) + 2;
			buf = new char[len];
			if (buf) {
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path, p->d_name);
				if (!stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = RemoveDirectoryRecursive(buf);
					} else {
						r2 = unlink(buf);
					}
				}
				delete[] buf;
			}
			r = r2;
		}
		closedir(d);
	}
	if (!r) {
		r = rmdir(path);
	}
	return r;
}

void LocalFileSystem::RemoveDirectory(const string &directory) {
	RemoveDirectoryRecursive(directory.c_str());
}

void LocalFileSystem::RemoveFile(const string &filename) {
	if (std::remove(filename.c_str()) != 0) {
		throw IOException("Could not remove file \"%s\": %s", filename, strerror(errno));
	}
}

bool LocalFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	if (!DirectoryExists(directory)) {
		return false;
	}
	DIR *dir = opendir(directory.c_str());
	if (!dir) {
		return false;
	}
	struct dirent *ent;
	// loop over all files in the directory
	while ((ent = readdir(dir)) != nullptr) {
		string name = string(ent->d_name);
		// skip . .. and empty files
		if (name.empty() || name == "." || name == "..") {
			continue;
		}
		// now stat the file to figure out if it is a regular file or directory
		string full_path = JoinPath(directory, name);
		if (access(full_path.c_str(), 0) != 0) {
			continue;
		}
		struct stat status;
		stat(full_path.c_str(), &status);
		if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
			// not a file or directory: skip
			continue;
		}
		// invoke callback
		callback(name, status.st_mode & S_IFDIR);
	}
	closedir(dir);
	return true;
}

void LocalFileSystem::FileSync(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (fsync(fd) != 0) {
		throw FatalException("fsync failed!");
	}
}

void LocalFileSystem::MoveFile(const string &source, const string &target) {
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(source.c_str(), target.c_str()) != 0) {
		throw IOException("Could not rename file!");
	}
}

std::string LocalFileSystem::GetLastErrorAsString() {
	return string();
}

#else

constexpr char PIPE_PREFIX[] = "\\\\.\\pipe\\";

// Returns the last Win32 error, in string format. Returns an empty string if there is no error.
std::string LocalFileSystem::GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = GetLastError();
	if (errorMessageID == 0)
		return std::string(); // No error message has been recorded

	LPSTR messageBuffer = nullptr;
	idx_t size =
	    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	                   NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

	std::string message(messageBuffer, size);

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}

struct WindowsFileHandle : public FileHandle {
public:
	WindowsFileHandle(FileSystem &file_system, string path, HANDLE fd)
	    : FileHandle(file_system, path), position(0), fd(fd) {
	}
	virtual ~WindowsFileHandle() {
		Close();
	}

	idx_t position;
	HANDLE fd;

public:
	void Close() override {
		if (!fd) {
			return;
		}
		CloseHandle(fd);
		fd = nullptr;
	};
};

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock_type,
                                                 FileCompressionType compression, FileOpener *opener) {
	if (compression != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}
	AssertValidFileFlags(flags);

	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;
	DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
	bool open_read = flags & FileFlags::FILE_FLAGS_READ;
	bool open_write = flags & FileFlags::FILE_FLAGS_WRITE;
	if (open_read && open_write) {
		desired_access = GENERIC_READ | GENERIC_WRITE;
		share_mode = 0;
	} else if (open_read) {
		desired_access = GENERIC_READ;
		share_mode = FILE_SHARE_READ;
	} else if (open_write) {
		desired_access = GENERIC_WRITE;
		share_mode = 0;
	} else {
		throw InternalException("READ, WRITE or both should be specified when opening a file");
	}
	if (open_write) {
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			creation_disposition = OPEN_ALWAYS;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			creation_disposition = CREATE_ALWAYS;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
		flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
	}
	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	HANDLE hFile = CreateFileW(unicode_path.c_str(), desired_access, share_mode, NULL, creation_disposition,
	                           flags_and_attributes, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Cannot open file \"%s\": %s", path.c_str(), error);
	}
	auto handle = make_unique<WindowsFileHandle>(*this, path.c_str(), hFile);
	if (flags & FileFlags::FILE_FLAGS_APPEND) {
		auto file_size = GetFileSize(*handle);
		SetFilePointer(*handle, file_size);
	}
	return move(handle);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	((WindowsFileHandle &)handle).position = location;
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	return ((WindowsFileHandle &)handle).position;
}

static DWORD FSInternalRead(FileHandle &handle, HANDLE hFile, void *buffer, int64_t nr_bytes, idx_t location) {
	DWORD bytes_read = 0;
	OVERLAPPED ov = {};
	ov.Internal = 0;
	ov.InternalHigh = 0;
	ov.Offset = location & 0xFFFFFFFF;
	ov.OffsetHigh = location >> 32;
	ov.hEvent = 0;
	auto rc = ReadFile(hFile, buffer, (DWORD)nr_bytes, &bytes_read, &ov);
	if (!rc) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Could not read file \"%s\" (error in ReadFile): %s", handle.path, error);
	}
	return bytes_read;
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto bytes_read = FSInternalRead(handle, hFile, buffer, nr_bytes, location);
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read all bytes from file \"%s\": wanted=%lld read=%lld", handle.path, nr_bytes,
		                  bytes_read);
	}
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto &pos = ((WindowsFileHandle &)handle).position;
	auto n = std::min<idx_t>(std::max<idx_t>(GetFileSize(handle), pos) - pos, nr_bytes);
	auto bytes_read = FSInternalRead(handle, hFile, buffer, n, pos);
	pos += bytes_read;
	return bytes_read;
}

static DWORD FSInternalWrite(FileHandle &handle, HANDLE hFile, void *buffer, int64_t nr_bytes, idx_t location) {
	DWORD bytes_written = 0;
	OVERLAPPED ov = {};
	ov.Internal = 0;
	ov.InternalHigh = 0;
	ov.Offset = location & 0xFFFFFFFF;
	ov.OffsetHigh = location >> 32;
	ov.hEvent = 0;
	auto rc = WriteFile(hFile, buffer, (DWORD)nr_bytes, &bytes_written, &ov);
	if (!rc) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Could not write file \"%s\" (error in WriteFile): %s", handle.path, error);
	}
	return bytes_written;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto bytes_written = FSInternalWrite(handle, hFile, buffer, nr_bytes, location);
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write all bytes from file \"%s\": wanted=%lld wrote=%lld", handle.path, nr_bytes,
		                  bytes_written);
	}
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto &pos = ((WindowsFileHandle &)handle).position;
	auto bytes_written = FSInternalWrite(handle, hFile, buffer, nr_bytes, pos);
	pos += bytes_written;
	return bytes_written;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	LARGE_INTEGER result;
	if (!GetFileSizeEx(hFile, &result)) {
		return -1;
	}
	return result.QuadPart;
}

time_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;

	// https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime
	FILETIME last_write;
	if (GetFileTime(hFile, nullptr, nullptr, &last_write) == 0) {
		return -1;
	}

	// https://stackoverflow.com/questions/29266743/what-is-dwlowdatetime-and-dwhighdatetime
	ULARGE_INTEGER ul;
	ul.LowPart = last_write.dwLowDateTime;
	ul.HighPart = last_write.dwHighDateTime;
	int64_t fileTime64 = ul.QuadPart;

	// fileTime64 contains a 64-bit value representing the number of
	// 100-nanosecond intervals since January 1, 1601 (UTC).
	// https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-filetime

	// Adapted from: https://stackoverflow.com/questions/6161776/convert-windows-filetime-to-second-in-unix-linux
	const auto WINDOWS_TICK = 10000000;
	const auto SEC_TO_UNIX_EPOCH = 11644473600LL;
	time_t result = (fileTime64 / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
	return result;
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	// seek to the location
	SetFilePointer(handle, new_size);
	// now set the end of file position
	if (!SetEndOfFile(hFile)) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failure in SetEndOfFile call on file \"%s\": %s", handle.path, error);
	}
}

static DWORD WindowsGetFileAttributes(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	return GetFileAttributesW(unicode_path.c_str());
}

bool LocalFileSystem::DirectoryExists(const string &directory) {
	DWORD attrs = WindowsGetFileAttributes(directory);
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void LocalFileSystem::CreateDirectory(const string &directory) {
	if (DirectoryExists(directory)) {
		return;
	}
	auto unicode_path = WindowsUtil::UTF8ToUnicode(directory.c_str());
	if (directory.empty() || !CreateDirectoryW(unicode_path.c_str(), NULL) || !DirectoryExists(directory)) {
		throw IOException("Could not create directory!");
	}
}

static void DeleteDirectoryRecursive(FileSystem &fs, string directory) {
	fs.ListFiles(directory, [&](const string &fname, bool is_directory) {
		if (is_directory) {
			DeleteDirectoryRecursive(fs, fs.JoinPath(directory, fname));
		} else {
			fs.RemoveFile(fs.JoinPath(directory, fname));
		}
	});
	auto unicode_path = WindowsUtil::UTF8ToUnicode(directory.c_str());
	if (!RemoveDirectoryW(unicode_path.c_str())) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to delete directory \"%s\": %s", directory, error);
	}
}

void LocalFileSystem::RemoveDirectory(const string &directory) {
	if (FileExists(directory)) {
		throw IOException("Attempting to delete directory \"%s\", but it is a file and not a directory!", directory);
	}
	if (!DirectoryExists(directory)) {
		return;
	}
	DeleteDirectoryRecursive(*this, directory.c_str());
}

void LocalFileSystem::RemoveFile(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	if (!DeleteFileW(unicode_path.c_str())) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to delete file \"%s\": %s", filename, error);
	}
}

bool LocalFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	string search_dir = JoinPath(directory, "*");

	auto unicode_path = WindowsUtil::UTF8ToUnicode(search_dir.c_str());

	WIN32_FIND_DATAW ffd;
	HANDLE hFind = FindFirstFileW(unicode_path.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
		string cFileName = WindowsUtil::UnicodeToUTF8(ffd.cFileName);
		if (cFileName == "." || cFileName == "..") {
			continue;
		}
		callback(cFileName, ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
	} while (FindNextFileW(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		FindClose(hFind);
		return false;
	}

	FindClose(hFind);
	return true;
}

void LocalFileSystem::FileSync(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	if (FlushFileBuffers(hFile) == 0) {
		throw IOException("Could not flush file handle to disk!");
	}
}

void LocalFileSystem::MoveFile(const string &source, const string &target) {
	auto source_unicode = WindowsUtil::UTF8ToUnicode(source.c_str());
	auto target_unicode = WindowsUtil::UTF8ToUnicode(target.c_str());
	if (!MoveFileW(source_unicode.c_str(), target_unicode.c_str())) {
		throw IOException("Could not move file");
	}
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	auto path = ((WindowsFileHandle &)handle).path;
	// pipes in windows are just files in '\\.\pipe\' folder
	if (strncmp(path.c_str(), PIPE_PREFIX, strlen(PIPE_PREFIX)) == 0) {
		return FileType::FILE_TYPE_FIFO;
	}
	DWORD attrs = WindowsGetFileAttributes(path.c_str());
	if (attrs != INVALID_FILE_ATTRIBUTES) {
		if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
			return FileType::FILE_TYPE_DIR;
		} else {
			return FileType::FILE_TYPE_REGULAR;
		}
	}
	return FileType::FILE_TYPE_INVALID;
}
#endif

bool LocalFileSystem::CanSeek() {
	return true;
}

bool LocalFileSystem::OnDiskFile(FileHandle &handle) {
	return true;
}

void LocalFileSystem::Seek(FileHandle &handle, idx_t location) {
	if (!CanSeek()) {
		throw IOException("Cannot seek in files of this type");
	}
	SetFilePointer(handle, location);
}

idx_t LocalFileSystem::SeekPosition(FileHandle &handle) {
	if (!CanSeek()) {
		throw IOException("Cannot seek in files of this type");
	}
	return GetFilePointer(handle);
}

static bool HasGlob(const string &str) {
	for (idx_t i = 0; i < str.size(); i++) {
		switch (str[i]) {
		case '*':
		case '?':
		case '[':
			return true;
		default:
			break;
		}
	}
	return false;
}

static void GlobFiles(FileSystem &fs, const string &path, const string &glob, bool match_directory,
                      vector<string> &result, bool join_path) {
	fs.ListFiles(path, [&](const string &fname, bool is_directory) {
		if (is_directory != match_directory) {
			return;
		}
		if (LikeFun::Glob(fname.c_str(), fname.size(), glob.c_str(), glob.size())) {
			if (join_path) {
				result.push_back(fs.JoinPath(path, fname));
			} else {
				result.push_back(fname);
			}
		}
	});
}

vector<string> LocalFileSystem::Glob(const string &path, FileOpener *opener) {
	if (path.empty()) {
		return vector<string>();
	}
	// split up the path into separate chunks
	vector<string> splits;
	idx_t last_pos = 0;
	for (idx_t i = 0; i < path.size(); i++) {
		if (path[i] == '\\' || path[i] == '/') {
			if (i == last_pos) {
				// empty: skip this position
				last_pos = i + 1;
				continue;
			}
			if (splits.empty()) {
				splits.push_back(path.substr(0, i));
			} else {
				splits.push_back(path.substr(last_pos, i - last_pos));
			}
			last_pos = i + 1;
		}
	}
	splits.push_back(path.substr(last_pos, path.size() - last_pos));
	// handle absolute paths
	bool absolute_path = false;
	if (path[0] == '/') {
		// first character is a slash -  unix absolute path
		absolute_path = true;
	} else if (StringUtil::Contains(splits[0], ":")) {
		// first split has a colon -  windows absolute path
		absolute_path = true;
	} else if (splits[0] == "~") {
		// starts with home directory
		auto home_directory = GetHomeDirectory();
		if (!home_directory.empty()) {
			absolute_path = true;
			splits[0] = home_directory;
		}
	}
	// Check if the path has a glob at all
	if (!HasGlob(path)) {
		// no glob: return only the file (if it exists or is a pipe)
		vector<string> result;
		if (FileExists(path) || IsPipe(path)) {
			result.push_back(path);
		} else if (!absolute_path) {
			Value value;
			if (opener->TryGetCurrentSetting("file_search_path", value)) {
				auto search_paths_str = value.ToString();
				std::vector<std::string> search_paths = StringUtil::Split(search_paths_str, ',');
				for (const auto &search_path : search_paths) {
					auto joined_path = JoinPath(search_path, path);
					if (FileExists(joined_path) || IsPipe(joined_path)) {
						result.push_back(joined_path);
					}
				}
			}
		}
		return result;
	}
	vector<string> previous_directories;
	if (absolute_path) {
		// for absolute paths, we don't start by scanning the current directory
		previous_directories.push_back(splits[0]);
	}
	for (idx_t i = absolute_path ? 1 : 0; i < splits.size(); i++) {
		bool is_last_chunk = i + 1 == splits.size();
		bool has_glob = HasGlob(splits[i]);
		// if it's the last chunk we need to find files, otherwise we find directories
		// not the last chunk: gather a list of all directories that match the glob pattern
		vector<string> result;
		if (!has_glob) {
			// no glob, just append as-is
			if (previous_directories.empty()) {
				result.push_back(splits[i]);
			} else {
				for (auto &prev_directory : previous_directories) {
					result.push_back(JoinPath(prev_directory, splits[i]));
				}
			}
		} else {
			if (previous_directories.empty()) {
				// no previous directories: list in the current path
				GlobFiles(*this, ".", splits[i], !is_last_chunk, result, false);
			} else {
				// previous directories
				// we iterate over each of the previous directories, and apply the glob of the current directory
				for (auto &prev_directory : previous_directories) {
					GlobFiles(*this, prev_directory, splits[i], !is_last_chunk, result, true);
				}
			}
		}
		if (is_last_chunk || result.empty()) {
			return result;
		}
		previous_directories = move(result);
	}
	return vector<string>();
}

unique_ptr<FileSystem> FileSystem::CreateLocal() {
	return make_unique<LocalFileSystem>();
}

} // namespace duckdb






















#include <cctype>
#include <cmath>
#include <cstdlib>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(bool input, bool &result, bool strict) {
	return NumericTryCast::Operation<bool, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<bool, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<bool, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, float &result, bool strict) {
	return NumericTryCast::Operation<bool, float>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, double &result, bool strict) {
	return NumericTryCast::Operation<bool, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<int64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<int64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<int64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<int64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict) {
	return NumericTryCast::Operation<hugeint_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint8_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint32_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint32_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint64_t input, bool &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, float &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, double &result, bool strict) {
	return NumericTryCast::Operation<uint64_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(float input, bool &result, bool strict) {
	return NumericTryCast::Operation<float, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<float, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<float, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<float, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<float, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<float, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<float, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, float &result, bool strict) {
	return NumericTryCast::Operation<float, float>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, double &result, bool strict) {
	return NumericTryCast::Operation<float, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(double input, bool &result, bool strict) {
	return NumericTryCast::Operation<double, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int8_t &result, bool strict) {
	return NumericTryCast::Operation<double, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int16_t &result, bool strict) {
	return NumericTryCast::Operation<double, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int32_t &result, bool strict) {
	return NumericTryCast::Operation<double, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int64_t &result, bool strict) {
	return NumericTryCast::Operation<double, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
	return NumericTryCast::Operation<double, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint8_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint16_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint32_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint64_t &result, bool strict) {
	return NumericTryCast::Operation<double, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, float &result, bool strict) {
	return NumericTryCast::Operation<double, float>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, double &result, bool strict) {
	return NumericTryCast::Operation<double, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <typename T>
struct IntegerCastData {
	using Result = T;
	Result result;
	bool seen_decimal;
};

struct IntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		using result_t = typename T::Result;
		if (NEGATIVE) {
			if (state.result < (NumericLimits<result_t>::Minimum() + digit) / 10) {
				return false;
			}
			state.result = state.result * 10 - digit;
		} else {
			if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 10) {
				return false;
			}
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		using result_t = typename T::Result;
		double dbl_res = state.result * std::pow(10.0L, exponent);
		if (dbl_res < NumericLimits<result_t>::Minimum() || dbl_res > NumericLimits<result_t>::Maximum()) {
			return false;
		}
		state.result = (result_t)std::nearbyint(dbl_res);
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.seen_decimal) {
			return true;
		}
		state.seen_decimal = true;
		// round the integer based on what is after the decimal point
		// if digit >= 5, then we round up (or down in case of negative numbers)
		auto increment = digit >= 5;
		if (!increment) {
			return true;
		}
		if (NEGATIVE) {
			if (state.result == NumericLimits<typename T::Result>::Minimum()) {
				return false;
			}
			state.result--;
		} else {
			if (state.result == NumericLimits<typename T::Result>::Maximum()) {
				return false;
			}
			state.result++;
		}
		return true;
	}

	template <class T>
	static bool Finalize(T &state) {
		return true;
	}
};

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerCastLoop(const char *buf, idx_t len, T &result, bool strict) {
	idx_t start_pos = NEGATIVE || *buf == '+' ? 1 : 0;
	idx_t pos = start_pos;
	while (pos < len) {
		if (!StringUtil::CharacterIsDigit(buf[pos])) {
			// not a digit!
			if (buf[pos] == '.') {
				if (strict) {
					return false;
				}
				bool number_before_period = pos > start_pos;
				// decimal point: we accept decimal values for integers as well
				// we just truncate them
				// make sure everything after the period is a number
				pos++;
				idx_t start_digit = pos;
				while (pos < len) {
					if (!StringUtil::CharacterIsDigit(buf[pos])) {
						break;
					}
					if (!OP::template HandleDecimal<T, NEGATIVE>(result, buf[pos] - '0')) {
						return false;
					}
					pos++;
				}
				// make sure there is either (1) one number after the period, or (2) one number before the period
				// i.e. we accept "1." and ".1" as valid numbers, but not "."
				if (!(number_before_period || pos > start_digit)) {
					return false;
				}
				if (pos >= len) {
					break;
				}
			}
			if (StringUtil::CharacterIsSpace(buf[pos])) {
				// skip any trailing spaces
				while (++pos < len) {
					if (!StringUtil::CharacterIsSpace(buf[pos])) {
						return false;
					}
				}
				break;
			}
			if (ALLOW_EXPONENT) {
				if (buf[pos] == 'e' || buf[pos] == 'E') {
					if (pos == start_pos) {
						return false;
					}
					pos++;
					if (pos >= len) {
						return false;
					}
					using ExponentData = IntegerCastData<int32_t>;
					ExponentData exponent {0, false};
					int negative = buf[pos] == '-';
					if (negative) {
						if (!IntegerCastLoop<ExponentData, true, false>(buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					} else {
						if (!IntegerCastLoop<ExponentData, false, false>(buf + pos, len - pos, exponent, strict)) {
							return false;
						}
					}
					return OP::template HandleExponent<T, NEGATIVE>(result, exponent.result);
				}
			}
			return false;
		}
		uint8_t digit = buf[pos++] - '0';
		if (!OP::template HandleDigit<T, NEGATIVE>(result, digit)) {
			return false;
		}
	}
	if (!OP::template Finalize<T>(result)) {
		return false;
	}
	return pos > start_pos;
}

template <class T, bool IS_SIGNED = true, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation,
          bool ZERO_INITIALIZE = true>
static bool TryIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	int negative = *buf == '-';

	if (ZERO_INITIALIZE) {
		memset(&result, 0, sizeof(T));
	}
	if (!negative) {
		return IntegerCastLoop<T, false, ALLOW_EXPONENT, OP>(buf, len, result, strict);
	} else {
		if (!IS_SIGNED) {
			// Need to check if its not -0
			idx_t pos = 1;
			while (pos < len) {
				if (buf[pos++] != '0') {
					return false;
				}
			}
		}
		return IntegerCastLoop<T, true, ALLOW_EXPONENT, OP>(buf, len, result, strict);
	}
}

template <typename T, bool IS_SIGNED = true>
static inline bool TrySimpleIntegerCast(const char *buf, idx_t len, T &result, bool strict) {
	IntegerCastData<T> data;
	if (TryIntegerCast<IntegerCastData<T>, IS_SIGNED>(buf, len, data, strict)) {
		result = data.result;
		return true;
	}
	return false;
}

template <>
bool TryCast::Operation(string_t input, bool &result, bool strict) {
	auto input_data = input.GetDataUnsafe();
	auto input_size = input.GetSize();

	switch (input_size) {
	case 1: {
		char c = std::tolower(*input_data);
		if (c == 't' || (!strict && c == '1')) {
			result = true;
			return true;
		} else if (c == 'f' || (!strict && c == '0')) {
			result = false;
			return true;
		}
		return false;
	}
	case 4: {
		char t = std::tolower(input_data[0]);
		char r = std::tolower(input_data[1]);
		char u = std::tolower(input_data[2]);
		char e = std::tolower(input_data[3]);
		if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
			result = true;
			return true;
		}
		return false;
	}
	case 5: {
		char f = std::tolower(input_data[0]);
		char a = std::tolower(input_data[1]);
		char l = std::tolower(input_data[2]);
		char s = std::tolower(input_data[3]);
		char e = std::tolower(input_data[4]);
		if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
			result = false;
			return true;
		}
		return false;
	}
	default:
		return false;
	}
}
template <>
bool TryCast::Operation(string_t input, int8_t &result, bool strict) {
	return TrySimpleIntegerCast<int8_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int16_t &result, bool strict) {
	return TrySimpleIntegerCast<int16_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int32_t &result, bool strict) {
	return TrySimpleIntegerCast<int32_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, int64_t &result, bool strict) {
	return TrySimpleIntegerCast<int64_t>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, uint8_t &result, bool strict) {
	return TrySimpleIntegerCast<uint8_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint16_t &result, bool strict) {
	return TrySimpleIntegerCast<uint16_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint32_t &result, bool strict) {
	return TrySimpleIntegerCast<uint32_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}
template <>
bool TryCast::Operation(string_t input, uint64_t &result, bool strict) {
	return TrySimpleIntegerCast<uint64_t, false>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

template <class T>
static bool TryDoubleCast(const char *buf, idx_t len, T &result, bool strict) {
	// skip any spaces at the start
	while (len > 0 && StringUtil::CharacterIsSpace(*buf)) {
		buf++;
		len--;
	}
	if (len == 0) {
		return false;
	}
	if (*buf == '+') {
		buf++;
		len--;
	}
	auto endptr = buf + len;
	auto parse_result = duckdb_fast_float::from_chars(buf, buf + len, result);
	if (parse_result.ec != std::errc()) {
		return false;
	}
	auto current_end = parse_result.ptr;
	if (!strict) {
		while (current_end < endptr && StringUtil::CharacterIsSpace(*current_end)) {
			current_end++;
		}
	}
	return current_end == endptr;
}

template <>
bool TryCast::Operation(string_t input, float &result, bool strict) {
	return TryDoubleCast<float>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

template <>
bool TryCast::Operation(string_t input, double &result, bool strict) {
	return TryDoubleCast<double>(input.GetDataUnsafe(), input.GetSize(), result, strict);
}

//===--------------------------------------------------------------------===//
// Cast From Date
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(date_t input, date_t &result, bool strict) {
	result = input;
	return true;
}

template <>
bool TryCast::Operation(date_t input, timestamp_t &result, bool strict) {
	if (input == date_t::infinity()) {
		result = timestamp_t::infinity();
		return true;
	} else if (input == date_t::ninfinity()) {
		result = timestamp_t::ninfinity();
		return true;
	}
	return Timestamp::TryFromDatetime(input, Time::FromTime(0, 0, 0), result);
}

//===--------------------------------------------------------------------===//
// Cast From Time
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(dtime_t input, dtime_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Timestamps
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(timestamp_t input, date_t &result, bool strict) {
	result = Timestamp::GetDate(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, dtime_t &result, bool strict) {
	if (!Timestamp::IsFinite(input)) {
		return false;
	}
	result = Timestamp::GetTime(input);
	return true;
}

template <>
bool TryCast::Operation(timestamp_t input, timestamp_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast from Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(interval_t input, interval_t &result, bool strict) {
	result = input;
	return true;
}

//===--------------------------------------------------------------------===//
// Non-Standard Timestamps
//===--------------------------------------------------------------------===//
template <>
duckdb::string_t CastFromTimestampNS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochNanoSeconds(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampMS::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochMs(input.value), result);
}
template <>
duckdb::string_t CastFromTimestampSec::Operation(duckdb::timestamp_t input, Vector &result) {
	return StringCast::Operation<timestamp_t>(Timestamp::FromEpochSeconds(input.value), result);
}

template <>
timestamp_t CastTimestampUsToMs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochMs(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToNs::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochNanoSeconds(input));
	return cast_timestamp;
}

template <>
timestamp_t CastTimestampUsToSec::Operation(timestamp_t input) {
	timestamp_t cast_timestamp(Timestamp::GetEpochSeconds(input));
	return cast_timestamp;
}
template <>
timestamp_t CastTimestampMsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochMs(input.value);
}

template <>
timestamp_t CastTimestampNsToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochNanoSeconds(input.value);
}

template <>
timestamp_t CastTimestampSecToUs::Operation(timestamp_t input) {
	return Timestamp::FromEpochSeconds(input.value);
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastToTimestampNS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochNanoSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochMs(result);
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(string_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result = Timestamp::GetEpochSeconds(result);
	return true;
}

template <>
bool TryCastToTimestampNS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	if (!TryMultiplyOperator::Operation(result.value, Interval::NANOS_PER_MICRO, result.value)) {
		return false;
	}
	return true;
}

template <>
bool TryCastToTimestampMS::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result.value /= Interval::MICROS_PER_MSEC;
	return true;
}

template <>
bool TryCastToTimestampSec::Operation(date_t input, timestamp_t &result, bool strict) {
	if (!TryCast::Operation<date_t, timestamp_t>(input, result, strict)) {
		return false;
	}
	result.value /= Interval::MICROS_PER_MSEC * Interval::MSECS_PER_SEC;
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From Blob
//===--------------------------------------------------------------------===//
template <>
string_t CastFromBlob::Operation(string_t input, Vector &vector) {
	idx_t result_size = Blob::GetStringSize(input);

	string_t result = StringVector::EmptyString(vector, result_size);
	Blob::ToString(input, result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To Blob
//===--------------------------------------------------------------------===//
template <>
bool TryCastToBlob::Operation(string_t input, string_t &result, Vector &result_vector, string *error_message,
                              bool strict) {
	idx_t result_size;
	if (!Blob::TryGetBlobSize(input, result_size, error_message)) {
		return false;
	}

	result = StringVector::EmptyString(result_vector, result_size);
	Blob::ToBlob(input, (data_ptr_t)result.GetDataWriteable());
	result.Finalize();
	return true;
}

//===--------------------------------------------------------------------===//
// Cast From UUID
//===--------------------------------------------------------------------===//
template <>
string_t CastFromUUID::Operation(hugeint_t input, Vector &vector) {
	string_t result = StringVector::EmptyString(vector, 36);
	UUID::ToString(input, result.GetDataWriteable());
	result.Finalize();
	return result;
}

//===--------------------------------------------------------------------===//
// Cast To UUID
//===--------------------------------------------------------------------===//
template <>
bool TryCastToUUID::Operation(string_t input, hugeint_t &result, Vector &result_vector, string *error_message,
                              bool strict) {
	return UUID::FromString(input.GetString(), result);
}

//===--------------------------------------------------------------------===//
// Cast To Date
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, date_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, date_t>(input, result, strict)) {
		HandleCastError::AssignError(Date::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, date_t &result, bool strict) {
	idx_t pos;
	return Date::TryConvertDate(input.GetDataUnsafe(), input.GetSize(), pos, result, strict);
}

template <>
date_t Cast::Operation(string_t input) {
	return Date::FromCString(input.GetDataUnsafe(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To Time
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, dtime_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, dtime_t>(input, result, strict)) {
		HandleCastError::AssignError(Time::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, dtime_t &result, bool strict) {
	idx_t pos;
	return Time::TryConvertTime(input.GetDataUnsafe(), input.GetSize(), pos, result, strict);
}

template <>
dtime_t Cast::Operation(string_t input) {
	return Time::FromCString(input.GetDataUnsafe(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast To Timestamp
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, timestamp_t &result, string *error_message, bool strict) {
	if (!TryCast::Operation<string_t, timestamp_t>(input, result, strict)) {
		HandleCastError::AssignError(Timestamp::ConversionError(input), error_message);
		return false;
	}
	return true;
}

template <>
bool TryCast::Operation(string_t input, timestamp_t &result, bool strict) {
	return Timestamp::TryConvertTimestamp(input.GetDataUnsafe(), input.GetSize(), result);
}

template <>
timestamp_t Cast::Operation(string_t input) {
	return Timestamp::FromCString(input.GetDataUnsafe(), input.GetSize());
}

//===--------------------------------------------------------------------===//
// Cast From Interval
//===--------------------------------------------------------------------===//
template <>
bool TryCastErrorMessage::Operation(string_t input, interval_t &result, string *error_message, bool strict) {
	return Interval::FromCString(input.GetDataUnsafe(), input.GetSize(), result, error_message, strict);
}

//===--------------------------------------------------------------------===//
// Cast From Hugeint
//===--------------------------------------------------------------------===//
// parsing hugeint from string is done a bit differently for performance reasons
// for other integer types we keep track of a single value
// and multiply that value by 10 for every digit we read
// however, for hugeints, multiplication is very expensive (>20X as expensive as for int64)
// for that reason, we parse numbers first into an int64 value
// when that value is full, we perform a HUGEINT multiplication to flush it into the hugeint
// this takes the number of HUGEINT multiplications down from [0-38] to [0-2]
struct HugeIntCastData {
	hugeint_t hugeint;
	int64_t intermediate;
	uint8_t digits;
	bool decimal;

	bool Flush() {
		if (digits == 0 && intermediate == 0) {
			return true;
		}
		if (hugeint.lower != 0 || hugeint.upper != 0) {
			if (digits > 38) {
				return false;
			}
			if (!Hugeint::TryMultiply(hugeint, Hugeint::POWERS_OF_TEN[digits], hugeint)) {
				return false;
			}
		}
		if (!Hugeint::AddInPlace(hugeint, hugeint_t(intermediate))) {
			return false;
		}
		digits = 0;
		intermediate = 0;
		return true;
	}
};

struct HugeIntegerCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &result, uint8_t digit) {
		if (NEGATIVE) {
			if (result.intermediate < (NumericLimits<int64_t>::Minimum() + digit) / 10) {
				// intermediate is full: need to flush it
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 - digit;
		} else {
			if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10) {
				if (!result.Flush()) {
					return false;
				}
			}
			result.intermediate = result.intermediate * 10 + digit;
		}
		result.digits++;
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &result, int32_t exponent) {
		if (!result.Flush()) {
			return false;
		}
		if (exponent < -38 || exponent > 38) {
			// out of range for exact exponent: use double and convert
			double dbl_res = Hugeint::Cast<double>(result.hugeint) * std::pow(10.0L, exponent);
			if (dbl_res < Hugeint::Cast<double>(NumericLimits<hugeint_t>::Minimum()) ||
			    dbl_res > Hugeint::Cast<double>(NumericLimits<hugeint_t>::Maximum())) {
				return false;
			}
			result.hugeint = Hugeint::Convert(dbl_res);
			return true;
		}
		if (exponent < 0) {
			// negative exponent: divide by power of 10
			result.hugeint = Hugeint::Divide(result.hugeint, Hugeint::POWERS_OF_TEN[-exponent]);
			return true;
		} else {
			// positive exponent: multiply by power of 10
			return Hugeint::TryMultiply(result.hugeint, Hugeint::POWERS_OF_TEN[exponent], result.hugeint);
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &result, uint8_t digit) {
		// Integer casts round
		if (!result.decimal) {
			if (!result.Flush()) {
				return false;
			}
			if (NEGATIVE) {
				result.intermediate = -(digit >= 5);
			} else {
				result.intermediate = (digit >= 5);
			}
		}
		result.decimal = true;

		return true;
	}

	template <class T>
	static bool Finalize(T &result) {
		return result.Flush();
	}
};

template <>
bool TryCast::Operation(string_t input, hugeint_t &result, bool strict) {
	HugeIntCastData data;
	if (!TryIntegerCast<HugeIntCastData, true, true, HugeIntegerCastOperation>(input.GetDataUnsafe(), input.GetSize(),
	                                                                           data, strict)) {
		return false;
	}
	result = data.hugeint;
	return true;
}

//===--------------------------------------------------------------------===//
// Decimal String Cast
//===--------------------------------------------------------------------===//
template <class T>
struct DecimalCastData {
	T result;
	uint8_t width;
	uint8_t scale;
	uint8_t digit_count;
	uint8_t decimal_count;
};

struct DecimalCastOperation {
	template <class T, bool NEGATIVE>
	static bool HandleDigit(T &state, uint8_t digit) {
		if (state.result == 0 && digit == 0) {
			// leading zero's don't count towards the digit count
			return true;
		}
		if (state.digit_count == state.width - state.scale) {
			// width of decimal type is exceeded!
			return false;
		}
		state.digit_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T, bool NEGATIVE>
	static bool HandleExponent(T &state, int32_t exponent) {
		Finalize<T>(state);
		if (exponent < 0) {
			for (idx_t i = 0; i < idx_t(-int64_t(exponent)); i++) {
				state.result /= 10;
				if (state.result == 0) {
					break;
				}
			}
			return true;
		} else {
			// positive exponent: append 0's
			for (idx_t i = 0; i < idx_t(exponent); i++) {
				if (!HandleDigit<T, NEGATIVE>(state, 0)) {
					return false;
				}
			}
			return true;
		}
	}

	template <class T, bool NEGATIVE>
	static bool HandleDecimal(T &state, uint8_t digit) {
		if (state.decimal_count == state.scale) {
			// we exceeded the amount of supported decimals
			// however, we don't throw an error here
			// we just truncate the decimal
			return true;
		}
		state.decimal_count++;
		if (NEGATIVE) {
			state.result = state.result * 10 - digit;
		} else {
			state.result = state.result * 10 + digit;
		}
		return true;
	}

	template <class T>
	static bool Finalize(T &state) {
		// if we have not gotten exactly "scale" decimals, we need to multiply the result
		// e.g. if we have a string "1.0" that is cast to a DECIMAL(9,3), the value needs to be 1000
		// but we have only gotten the value "10" so far, so we multiply by 1000
		for (uint8_t i = state.decimal_count; i < state.scale; i++) {
			state.result *= 10;
		}
		return true;
	}
};

template <class T>
bool TryDecimalStringCast(string_t input, T &result, string *error_message, uint8_t width, uint8_t scale) {
	DecimalCastData<T> state;
	state.result = 0;
	state.width = width;
	state.scale = scale;
	state.digit_count = 0;
	state.decimal_count = 0;
	if (!TryIntegerCast<DecimalCastData<T>, true, true, DecimalCastOperation, false>(input.GetDataUnsafe(),
	                                                                                 input.GetSize(), state, false)) {
		string error = StringUtil::Format("Could not convert string \"%s\" to DECIMAL(%d,%d)", input.GetString(),
		                                  (int)width, (int)scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = state.result;
	return true;
}

template <>
bool TryCastToDecimal::Operation(string_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryDecimalStringCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(string_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return TryDecimalStringCast<hugeint_t>(input, result, error_message, width, scale);
}

template <>
string_t StringCastFromDecimal::Operation(int16_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int16_t, uint16_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int32_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int32_t, uint32_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(int64_t input, uint8_t width, uint8_t scale, Vector &result) {
	return DecimalToString::Format<int64_t, uint64_t>(input, scale, result);
}

template <>
string_t StringCastFromDecimal::Operation(hugeint_t input, uint8_t width, uint8_t scale, Vector &result) {
	return HugeintToStringCast::FormatDecimal(input, scale, result);
}

//===--------------------------------------------------------------------===//
// Decimal Casts
//===--------------------------------------------------------------------===//
// Decimal <-> Bool
//===--------------------------------------------------------------------===//
template <class T, class OP = NumericHelper>
bool TryCastBoolToDecimal(bool input, T &result, string *error_message, uint8_t width, uint8_t scale) {
	if (width > scale) {
		result = input ? OP::POWERS_OF_TEN[scale] : 0;
		return true;
	} else {
		return TryCast::Operation<bool, T>(input, result);
	}
}

template <>
bool TryCastToDecimal::Operation(bool input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(bool input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastBoolToDecimal<hugeint_t, Hugeint>(input, result, error_message, width, scale);
}

template <>
bool TryCastFromDecimal::Operation(int16_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int16_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int32_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<int64_t, bool>(input, result);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, bool &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCast::Operation<hugeint_t, bool>(input, result);
}

//===--------------------------------------------------------------------===//
// Numeric -> Decimal Cast
//===--------------------------------------------------------------------===//
struct SignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return int64_t(input) >= int64_t(max_width) || int64_t(input) <= int64_t(-max_width);
	}
};

struct UnsignedToDecimalOperator {
	template <class SRC, class DST>
	static bool Operation(SRC input, DST max_width) {
		return uint64_t(input) >= uint64_t(max_width);
	}
};

template <class SRC, class DST, class OP = SignedToDecimalOperator>
bool StandardNumericToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	DST max_width = NumericHelper::POWERS_OF_TEN[width - scale];
	if (OP::template Operation<SRC, DST>(input, max_width)) {
		string error = StringUtil::Format("Could not cast value %d to DECIMAL(%d,%d)", input, width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = DST(input) * NumericHelper::POWERS_OF_TEN[scale];
	return true;
}

template <class SRC>
bool NumericToHugeDecimalCast(SRC input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	hugeint_t hinput = Hugeint::Convert(input);
	if (hinput >= max_width || hinput <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", hinput.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = hinput * Hugeint::POWERS_OF_TEN[scale];
	return true;
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int8_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int8_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int8_t input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return NumericToHugeDecimalCast<int8_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int16_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int16_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int32_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int32_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int16_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int32_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<int64_t, int64_t>(input, result, error_message, width, scale);
}
template <>
bool TryCastToDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<int64_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint8_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint8_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                 width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint8_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint8_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint16_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint16_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint16_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint32_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint32_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint32_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Decimal
//===--------------------------------------------------------------------===//
template <>
bool TryCastToDecimal::Operation(uint64_t input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int16_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int32_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return StandardNumericToDecimalCast<uint64_t, int64_t, UnsignedToDecimalOperator>(input, result, error_message,
	                                                                                  width, scale);
}
template <>
bool TryCastToDecimal::Operation(uint64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return NumericToHugeDecimalCast<uint64_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Hugeint -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class DST>
bool HugeintToDecimalCast(hugeint_t input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	// check for overflow
	hugeint_t max_width = Hugeint::POWERS_OF_TEN[width - scale];
	if (input >= max_width || input <= -max_width) {
		string error = StringUtil::Format("Could not cast value %s to DECIMAL(%d,%d)", input.ToString(), width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Hugeint::Cast<DST>(input * Hugeint::POWERS_OF_TEN[scale]);
	return true;
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                 uint8_t scale) {
	return HugeintToDecimalCast<hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Float/Double -> Decimal Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool DoubleToDecimalCast(SRC input, DST &result, string *error_message, uint8_t width, uint8_t scale) {
	double value = input * NumericHelper::DOUBLE_POWERS_OF_TEN[scale];
	// Add the sign (-1, 0, 1) times a tiny value to fix floating point issues (issue 3091)
	double sign = (double(0) < value) - (value < double(0));
	value += 1e-9 * sign;
	if (value <= -NumericHelper::DOUBLE_POWERS_OF_TEN[width] || value >= NumericHelper::DOUBLE_POWERS_OF_TEN[width]) {
		string error = StringUtil::Format("Could not cast value %f to DECIMAL(%d,%d)", value, width, scale);
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	result = Cast::Operation<SRC, DST>(value);
	return true;
}

template <>
bool TryCastToDecimal::Operation(float input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(float input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<float, hugeint_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int16_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int16_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int32_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int32_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, int64_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, int64_t>(input, result, error_message, width, scale);
}

template <>
bool TryCastToDecimal::Operation(double input, hugeint_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return DoubleToDecimalCast<double, hugeint_t>(input, result, error_message, width, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Numeric Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToNumeric(SRC input, DST &result, string *error_message, uint8_t scale) {
	// Round away from 0.
	const auto power = NumericHelper::POWERS_OF_TEN[scale];
	// https://graphics.stanford.edu/~seander/bithacks.html#ConditionalNegate
	const auto fNegate = int64_t(input < 0);
	const auto rounding = ((power ^ -fNegate) + fNegate) / 2;
	const auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<SRC, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %d to type %s", scaled_value, GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

template <class DST>
bool TryCastHugeDecimalToNumeric(hugeint_t input, DST &result, string *error_message, uint8_t scale) {
	const auto power = Hugeint::POWERS_OF_TEN[scale];
	const auto rounding = ((input < 0) ? -power : power) / 2;
	auto scaled_value = (input + rounding) / power;
	if (!TryCast::Operation<hugeint_t, DST>(scaled_value, result)) {
		string error = StringUtil::Format("Failed to cast decimal value %s to type %s",
		                                  ConvertToString::Operation(scaled_value), GetTypeId<DST>());
		HandleCastError::AssignError(error, error_message);
		return false;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int8_t &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int8_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int16_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int32_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> int64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, int64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, int64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<int64_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint8_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint8_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint8_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint8_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint16_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint16_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint16_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint16_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint32_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint32_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint32_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint32_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> uint64_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, uint64_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, uint64_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<uint64_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Cast Decimal -> hugeint_t
//===--------------------------------------------------------------------===//
template <>
bool TryCastFromDecimal::Operation(int16_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int16_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int32_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int32_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(int64_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToNumeric<int64_t, hugeint_t>(input, result, error_message, scale);
}
template <>
bool TryCastFromDecimal::Operation(hugeint_t input, hugeint_t &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastHugeDecimalToNumeric<hugeint_t>(input, result, error_message, scale);
}

//===--------------------------------------------------------------------===//
// Decimal -> Float/Double Cast
//===--------------------------------------------------------------------===//
template <class SRC, class DST>
bool TryCastDecimalToFloatingPoint(SRC input, DST &result, uint8_t scale) {
	result = Cast::Operation<SRC, DST>(input) / DST(NumericHelper::DOUBLE_POWERS_OF_TEN[scale]);
	return true;
}

// DECIMAL -> FLOAT
template <>
bool TryCastFromDecimal::Operation(int16_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, float &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, float>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, float &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, float>(input, result, scale);
}

// DECIMAL -> DOUBLE
template <>
bool TryCastFromDecimal::Operation(int16_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int16_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int32_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int32_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(int64_t input, double &result, string *error_message, uint8_t width, uint8_t scale) {
	return TryCastDecimalToFloatingPoint<int64_t, double>(input, result, scale);
}

template <>
bool TryCastFromDecimal::Operation(hugeint_t input, double &result, string *error_message, uint8_t width,
                                   uint8_t scale) {
	return TryCastDecimalToFloatingPoint<hugeint_t, double>(input, result, scale);
}

} // namespace duckdb




namespace duckdb {

template <class T>
string StandardStringCast(T input) {
	Vector v(LogicalType::VARCHAR);
	return StringCast::Operation(input, v).GetString();
}

template <>
string ConvertToString::Operation(bool input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(int64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint8_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint16_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint32_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(uint64_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(hugeint_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(float input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(double input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(interval_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(date_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(dtime_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(timestamp_t input) {
	return StandardStringCast(input);
}
template <>
string ConvertToString::Operation(string_t input) {
	return input.GetString();
}

} // namespace duckdb











namespace duckdb {

//===--------------------------------------------------------------------===//
// Cast Numeric -> String
//===--------------------------------------------------------------------===//
template <>
string_t StringCast::Operation(bool input, Vector &vector) {
	if (input) {
		return StringVector::AddString(vector, "true", 4);
	} else {
		return StringVector::AddString(vector, "false", 5);
	}
}

template <>
string_t StringCast::Operation(int8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int8_t, uint8_t>(input, vector);
}

template <>
string_t StringCast::Operation(int16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int16_t, uint16_t>(input, vector);
}
template <>
string_t StringCast::Operation(int32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int32_t, uint32_t>(input, vector);
}

template <>
string_t StringCast::Operation(int64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<int64_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint8_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint8_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint16_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint16_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint32_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint32_t, uint64_t>(input, vector);
}
template <>
duckdb::string_t StringCast::Operation(uint64_t input, Vector &vector) {
	return NumericHelper::FormatSigned<uint64_t, uint64_t>(input, vector);
}

template <>
string_t StringCast::Operation(float input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(double input, Vector &vector) {
	std::string s = duckdb_fmt::format("{}", input);
	return StringVector::AddString(vector, s);
}

template <>
string_t StringCast::Operation(interval_t input, Vector &vector) {
	char buffer[70];
	idx_t length = IntervalToStringCast::Format(input, buffer);
	return StringVector::AddString(vector, buffer, length);
}

template <>
duckdb::string_t StringCast::Operation(hugeint_t input, Vector &vector) {
	return HugeintToStringCast::FormatSigned(input, vector);
}

template <>
duckdb::string_t StringCast::Operation(date_t input, Vector &vector) {
	if (input == date_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == date_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	int32_t date[3];
	Date::Convert(input, date[0], date[1], date[2]);

	idx_t year_length;
	bool add_bc;
	idx_t length = DateToStringCast::Length(date, year_length, add_bc);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	char micro_buffer[10];
	idx_t length = TimeToStringCast::Length(time, micro_buffer);

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	TimeToStringCast::Format(data, length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamp is DATE TIME (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	idx_t length = date_length + time_length + 1;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	DateToStringCast::Format(data, date, year_length, add_bc);
	data[date_length] = ' ';
	TimeToStringCast::Format(data + date_length + 1, time_length, time, micro_buffer);

	result.Finalize();
	return result;
}

template <>
duckdb::string_t StringCast::Operation(duckdb::string_t input, Vector &result) {
	return StringVector::AddStringOrBlob(result, input);
}

template <>
string_t StringCastTZ::Operation(dtime_t input, Vector &vector) {
	int32_t time[4];
	Time::Convert(input, time[0], time[1], time[2], time[3]);

	// format for timetz is TIME+00
	char micro_buffer[10];
	const auto time_length = TimeToStringCast::Length(time, micro_buffer);
	const idx_t length = time_length + 3;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	TimeToStringCast::Format(data + pos, length, time, micro_buffer);
	pos += time_length;
	data[pos++] = '+';
	data[pos++] = '0';
	data[pos++] = '0';

	result.Finalize();
	return result;
}

template <>
string_t StringCastTZ::Operation(timestamp_t input, Vector &vector) {
	if (input == timestamp_t::infinity()) {
		return StringVector::AddString(vector, Date::PINF);
	} else if (input == timestamp_t::ninfinity()) {
		return StringVector::AddString(vector, Date::NINF);
	}
	date_t date_entry;
	dtime_t time_entry;
	Timestamp::Convert(input, date_entry, time_entry);

	int32_t date[3], time[4];
	Date::Convert(date_entry, date[0], date[1], date[2]);
	Time::Convert(time_entry, time[0], time[1], time[2], time[3]);

	// format for timestamptz is DATE TIME+00 (separated by space)
	idx_t year_length;
	bool add_bc;
	char micro_buffer[6];
	const idx_t date_length = DateToStringCast::Length(date, year_length, add_bc);
	const idx_t time_length = TimeToStringCast::Length(time, micro_buffer);
	const idx_t length = date_length + 1 + time_length + 3;

	string_t result = StringVector::EmptyString(vector, length);
	auto data = result.GetDataWriteable();

	idx_t pos = 0;
	DateToStringCast::Format(data + pos, date, year_length, add_bc);
	pos += date_length;
	data[pos++] = ' ';
	TimeToStringCast::Format(data + pos, time_length, time, micro_buffer);
	pos += time_length;
	data[pos++] = '+';
	data[pos++] = '0';
	data[pos++] = '0';

	result.Finalize();
	return result;
}

} // namespace duckdb




namespace duckdb {
class PipeFile : public FileHandle {
public:
	PipeFile(unique_ptr<FileHandle> child_handle_p, const string &path)
	    : FileHandle(pipe_fs, path), child_handle(move(child_handle_p)) {
	}

	PipeFileSystem pipe_fs;
	unique_ptr<FileHandle> child_handle;

public:
	int64_t ReadChunk(void *buffer, int64_t nr_bytes);
	int64_t WriteChunk(void *buffer, int64_t nr_bytes);

	void Close() override {
	}
};

int64_t PipeFile::ReadChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Read(buffer, nr_bytes);
}
int64_t PipeFile::WriteChunk(void *buffer, int64_t nr_bytes) {
	return child_handle->Write(buffer, nr_bytes);
}

void PipeFileSystem::Reset(FileHandle &handle) {
	throw InternalException("Cannot reset pipe file system");
}

int64_t PipeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = (PipeFile &)handle;
	return pipe.ReadChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &pipe = (PipeFile &)handle;
	return pipe.WriteChunk(buffer, nr_bytes);
}

int64_t PipeFileSystem::GetFileSize(FileHandle &handle) {
	return 0;
}

void PipeFileSystem::FileSync(FileHandle &handle) {
}

unique_ptr<FileHandle> PipeFileSystem::OpenPipe(unique_ptr<FileHandle> handle) {
	auto path = handle->path;
	return make_unique<PipeFile>(move(handle), path);
}

} // namespace duckdb




#include <stdio.h>

#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
#include <io.h>
#endif
#endif

namespace duckdb {

// LCOV_EXCL_START
void Printer::Print(const string &str) {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	if (IsTerminal()) {
		// print utf8 to terminal
		auto unicode = WindowsUtil::UTF8ToMBCS(str.c_str());
		fprintf(stderr, "%s\n", unicode.c_str());
		return;
	}
#endif
	fprintf(stderr, "%s\n", str.c_str());
#endif
}

void Printer::PrintProgress(int percentage, const char *pbstr, int pbwidth) {
#ifndef DUCKDB_DISABLE_PRINT
	int lpad = (int)(percentage / 100.0 * pbwidth);
	int rpad = pbwidth - lpad;
	printf("\r%3d%% [%.*s%*s]", percentage, lpad, pbstr, rpad, "");
	fflush(stdout);
#endif
}

void Printer::FinishProgressBarPrint(const char *pbstr, int pbwidth) {
#ifndef DUCKDB_DISABLE_PRINT
	PrintProgress(100, pbstr, pbwidth);
	printf(" \n");
	fflush(stdout);
#endif
}

bool Printer::IsTerminal() {
#ifndef DUCKDB_DISABLE_PRINT
#ifdef DUCKDB_WINDOWS
	return GetFileType(GetStdHandle(STD_ERROR_HANDLE)) == FILE_TYPE_CHAR;
#else
	throw InternalException("IsTerminal is only implemented for Windows");
#endif
#endif
	return false;
}
// LCOV_EXCL_STOP

} // namespace duckdb




namespace duckdb {

ProgressBar::ProgressBar(Executor &executor, idx_t show_progress_after, bool print_progress)
    : executor(executor), show_progress_after(show_progress_after), current_percentage(-1),
      print_progress(print_progress) {
}

double ProgressBar::GetCurrentPercentage() {
	return current_percentage;
}

void ProgressBar::Start() {
	profiler.Start();
	current_percentage = 0;
	supported = true;
}

void ProgressBar::Update(bool final) {
	if (!supported) {
		return;
	}
	double new_percentage;
	supported = executor.GetPipelinesProgress(new_percentage);
	if (!supported) {
		return;
	}
	auto sufficient_time_elapsed = profiler.Elapsed() > show_progress_after / 1000.0;
	if (new_percentage > current_percentage) {
		current_percentage = new_percentage;
	}
	if (supported && print_progress && sufficient_time_elapsed && current_percentage > -1 && print_progress) {
		if (final) {
			Printer::FinishProgressBarPrint(PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		} else {
			Printer::PrintProgress(current_percentage, PROGRESS_BAR_STRING.c_str(), PROGRESS_BAR_WIDTH);
		}
	}
}

} // namespace duckdb


#include <random>

namespace duckdb {

struct RandomState {
	RandomState() {
	}

	pcg32 pcg;
};

RandomEngine::RandomEngine(int64_t seed) : random_state(make_unique<RandomState>()) {
	if (seed < 0) {
		random_state->pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
	} else {
		random_state->pcg.seed(seed);
	}
}

RandomEngine::~RandomEngine() {
}

double RandomEngine::NextRandom(double min, double max) {
	D_ASSERT(max >= min);
	return min + (NextRandom() * (max - min));
}

double RandomEngine::NextRandom() {
	return random_state->pcg() / double(std::numeric_limits<uint32_t>::max());
}
uint32_t RandomEngine::NextRandomInteger() {
	return random_state->pcg();
}

void RandomEngine::SetSeed(uint32_t seed) {
	random_state->pcg.seed(seed);
}

} // namespace duckdb
#include <vector>
#include <memory>




namespace duckdb_re2 {

Regex::Regex(const std::string &pattern, RegexOptions options) {
	RE2::Options o;
	o.set_case_sensitive(options == RegexOptions::CASE_INSENSITIVE);
	regex = std::make_shared<duckdb_re2::RE2>(StringPiece(pattern), o);
}

bool RegexSearchInternal(const char *input, Match &match, const Regex &r, RE2::Anchor anchor, size_t start,
                         size_t end) {
	auto &regex = r.GetRegex();
	std::vector<StringPiece> target_groups;
	auto group_count = regex.NumberOfCapturingGroups() + 1;
	target_groups.resize(group_count);
	match.groups.clear();
	if (!regex.Match(StringPiece(input), start, end, anchor, target_groups.data(), group_count)) {
		return false;
	}
	for (auto &group : target_groups) {
		GroupMatch group_match;
		group_match.text = group.ToString();
		group_match.position = group.data() - input;
		match.groups.emplace_back(group_match);
	}
	return true;
}

bool RegexSearch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, 0, input.size());
}

bool RegexMatch(const std::string &input, Match &match, const Regex &regex) {
	return RegexSearchInternal(input.c_str(), match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

bool RegexMatch(const char *start, const char *end, Match &match, const Regex &regex) {
	return RegexSearchInternal(start, match, regex, RE2::ANCHOR_BOTH, 0, end - start);
}

bool RegexMatch(const std::string &input, const Regex &regex) {
	Match nop_match;
	return RegexSearchInternal(input.c_str(), nop_match, regex, RE2::ANCHOR_BOTH, 0, input.size());
}

std::vector<Match> RegexFindAll(const std::string &input, const Regex &regex) {
	std::vector<Match> matches;
	size_t position = 0;
	Match match;
	while (RegexSearchInternal(input.c_str(), match, regex, RE2::UNANCHORED, position, input.size())) {
		position += match.position(0) + match.length(0);
		matches.emplace_back(std::move(match));
	}
	return matches;
}

} // namespace duckdb_re2
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_aggregate.cpp
//
//
//===----------------------------------------------------------------------===//







namespace duckdb {

void RowOperations::InitializeStates(RowLayout &layout, Vector &addresses, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}
	auto pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto &offsets = layout.GetOffsets();
	auto aggr_idx = layout.ColumnCount();

	for (auto &aggr : layout.GetAggregates()) {
		for (idx_t i = 0; i < count; ++i) {
			auto row_idx = sel.get_index(i);
			auto row = pointers[row_idx];
			aggr.function.initialize(row + offsets[aggr_idx]);
		}
		++aggr_idx;
	}
}

void RowOperations::DestroyStates(RowLayout &layout, Vector &addresses, idx_t count) {
	if (count == 0) {
		return;
	}
	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), count);
	for (auto &aggr : layout.GetAggregates()) {
		if (aggr.function.destructor) {
			aggr.function.destructor(addresses, count);
		}
		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses, aggr.payload_size, count);
	}
}

void RowOperations::UpdateStates(AggregateObject &aggr, Vector &addresses, DataChunk &payload, idx_t arg_idx,
                                 idx_t count) {
	AggregateInputData aggr_input_data(aggr.bind_data);
	aggr.function.update(aggr.child_count == 0 ? nullptr : &payload.data[arg_idx], aggr_input_data, aggr.child_count,
	                     addresses, count);
}

void RowOperations::UpdateFilteredStates(AggregateFilterData &filter_data, AggregateObject &aggr, Vector &addresses,
                                         DataChunk &payload, idx_t arg_idx) {
	idx_t count = filter_data.ApplyFilter(payload);

	Vector filtered_addresses(addresses, filter_data.true_sel, count);
	filtered_addresses.Normalify(count);

	UpdateStates(aggr, filtered_addresses, filter_data.filtered_payload, arg_idx, count);
}

void RowOperations::CombineStates(RowLayout &layout, Vector &sources, Vector &targets, idx_t count) {
	if (count == 0) {
		return;
	}

	//	Move to the first aggregate states
	VectorOperations::AddInPlace(sources, layout.GetAggrOffset(), count);
	VectorOperations::AddInPlace(targets, layout.GetAggrOffset(), count);
	for (auto &aggr : layout.GetAggregates()) {
		D_ASSERT(aggr.function.combine);
		AggregateInputData aggr_input_data(aggr.bind_data);
		aggr.function.combine(sources, targets, aggr_input_data, count);

		// Move to the next aggregate states
		VectorOperations::AddInPlace(sources, aggr.payload_size, count);
		VectorOperations::AddInPlace(targets, aggr.payload_size, count);
	}
}

void RowOperations::FinalizeStates(RowLayout &layout, Vector &addresses, DataChunk &result, idx_t aggr_idx) {
	//	Move to the first aggregate state
	VectorOperations::AddInPlace(addresses, layout.GetAggrOffset(), result.size());

	auto &aggregates = layout.GetAggregates();
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &target = result.data[aggr_idx + i];
		auto &aggr = aggregates[i];
		AggregateInputData aggr_input_data(aggr.bind_data);
		aggr.function.finalize(addresses, aggr_input_data, target, result.size(), 0);

		// Move to the next aggregate state
		VectorOperations::AddInPlace(addresses, aggr.payload_size, result.size());
	}
}

} // namespace duckdb
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_operations/row_external.cpp
//
//
//===----------------------------------------------------------------------===//



namespace duckdb {

void RowOperations::SwizzleColumns(const RowLayout &layout, const data_ptr_t base_row_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Load heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = Load<data_ptr_t>(heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + sizeof(uint32_t) + string_t::PREFIX_LENGTH;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string pointer with the within-row offset (if not inlined)
						Store<idx_t>(Load<data_ptr_t>(string_ptr) - heap_row_ptrs[i], string_ptr);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data pointer with the within-row offset
					Store<idx_t>(Load<data_ptr_t>(col_ptr) - heap_row_ptrs[i], col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

void RowOperations::SwizzleHeapPointer(const RowLayout &layout, data_ptr_t row_ptr, const data_ptr_t heap_base_ptr,
                                       const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	row_ptr += layout.GetHeapPointerOffset();
	idx_t cumulative_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		Store<idx_t>(cumulative_offset, row_ptr);
		cumulative_offset += Load<uint32_t>(heap_base_ptr + cumulative_offset);
		row_ptr += row_width;
	}
}

void RowOperations::UnswizzlePointers(const RowLayout &layout, const data_ptr_t base_row_ptr,
                                      const data_ptr_t base_heap_ptr, const idx_t count) {
	const idx_t row_width = layout.GetRowWidth();
	data_ptr_t heap_row_ptrs[STANDARD_VECTOR_SIZE];
	idx_t done = 0;
	while (done != count) {
		const idx_t next = MinValue<idx_t>(count - done, STANDARD_VECTOR_SIZE);
		const data_ptr_t row_ptr = base_row_ptr + done * row_width;
		// Restore heap row pointers
		data_ptr_t heap_ptr_ptr = row_ptr + layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < next; i++) {
			heap_row_ptrs[i] = base_heap_ptr + Load<idx_t>(heap_ptr_ptr);
			Store<data_ptr_t>(heap_row_ptrs[i], heap_ptr_ptr);
			heap_ptr_ptr += row_width;
		}
		// Loop through the blob columns
		for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
			auto physical_type = layout.GetTypes()[col_idx].InternalType();
			if (TypeIsConstantSize(physical_type)) {
				continue;
			}
			data_ptr_t col_ptr = row_ptr + layout.GetOffsets()[col_idx];
			if (physical_type == PhysicalType::VARCHAR) {
				data_ptr_t string_ptr = col_ptr + sizeof(uint32_t) + string_t::PREFIX_LENGTH;
				for (idx_t i = 0; i < next; i++) {
					if (Load<uint32_t>(col_ptr) > string_t::INLINE_LENGTH) {
						// Overwrite the string offset with the pointer (if not inlined)
						Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(string_ptr), string_ptr);
					}
					col_ptr += row_width;
					string_ptr += row_width;
				}
			} else {
				// Non-varchar blob columns
				for (idx_t i = 0; i < next; i++) {
					// Overwrite the column data offset with the pointer
					Store<data_ptr_t>(heap_row_ptrs[i] + Load<idx_t>(col_ptr), col_ptr);
					col_ptr += row_width;
				}
			}
		}
		done += next;
	}
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// row_gather.cpp
// Description: This file contains the implementation of the gather operators
//===--------------------------------------------------------------------===//







namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedGatherLoop(Vector &rows, const SelectionVector &row_sel, Vector &col,
                                const SelectionVector &col_sel, idx_t count, idx_t col_offset, idx_t col_no,
                                idx_t build_size) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		auto row = ptrs[row_idx];
		auto col_idx = col_sel.get_index(i);
		data[col_idx] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			if (build_size > STANDARD_VECTOR_SIZE && col_mask.AllValid()) {
				//! We need to initialize the mask with the vector size.
				col_mask.Initialize(build_size);
			}
			col_mask.SetInvalid(col_idx);
		}
	}
}

static void GatherNestedVector(Vector &rows, const SelectionVector &row_sel, Vector &col,
                               const SelectionVector &col_sel, idx_t count, idx_t col_offset, idx_t col_no) {
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	// Build the gather locations
	auto data_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[count]);
	auto mask_locations = unique_ptr<data_ptr_t[]>(new data_ptr_t[count]);
	for (idx_t i = 0; i < count; i++) {
		auto row_idx = row_sel.get_index(i);
		mask_locations[i] = ptrs[row_idx];
		data_locations[i] = Load<data_ptr_t>(ptrs[row_idx] + col_offset);
	}

	// Deserialise into the selected locations
	RowOperations::HeapGather(col, count, col_sel, col_no, data_locations.get(), mask_locations.get());
}

void RowOperations::Gather(Vector &rows, const SelectionVector &row_sel, Vector &col, const SelectionVector &col_sel,
                           const idx_t count, const idx_t col_offset, const idx_t col_no, const idx_t build_size) {
	D_ASSERT(rows.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(rows.GetType().id() == LogicalTypeId::POINTER); // "Cannot gather from non-pointer type!"

	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedGatherLoop<uint8_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT16:
		TemplatedGatherLoop<uint16_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT32:
		TemplatedGatherLoop<uint32_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::UINT64:
		TemplatedGatherLoop<uint64_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedGatherLoop<int8_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT16:
		TemplatedGatherLoop<int16_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT32:
		TemplatedGatherLoop<int32_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT64:
		TemplatedGatherLoop<int64_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INT128:
		TemplatedGatherLoop<hugeint_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::FLOAT:
		TemplatedGatherLoop<float>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGatherLoop<double>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGatherLoop<interval_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::VARCHAR:
		TemplatedGatherLoop<string_t>(rows, row_sel, col, col_sel, count, col_offset, col_no, build_size);
		break;
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		GatherNestedVector(rows, row_sel, col, col_sel, count, col_offset, col_no);
		break;
	default:
		throw InternalException("Unimplemented type for RowOperations::Gather");
	}
}

template <class T>
static void TemplatedFullScanLoop(Vector &rows, Vector &col, idx_t count, idx_t col_offset, idx_t col_no) {
	// Precompute mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	auto data = FlatVector::GetData<T>(col);
	//	auto &col_mask = FlatVector::Validity(col);

	for (idx_t i = 0; i < count; i++) {
		auto row = ptrs[i];
		data[i] = Load<T>(row + col_offset);
		ValidityBytes row_mask(row);
		if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
			throw InternalException("Null value comparisons not implemented for perfect hash table yet");
			//			col_mask.SetInvalid(i);
		}
	}
}

void RowOperations::FullScanColumn(const RowLayout &layout, Vector &rows, Vector &col, idx_t count, idx_t col_no) {
	const auto col_offset = layout.GetOffsets()[col_no];
	col.SetVectorType(VectorType::FLAT_VECTOR);
	switch (col.GetType().InternalType()) {
	case PhysicalType::UINT8:
		TemplatedFullScanLoop<uint8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT16:
		TemplatedFullScanLoop<uint16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT32:
		TemplatedFullScanLoop<uint32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::UINT64:
		TemplatedFullScanLoop<uint64_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT8:
		TemplatedFullScanLoop<int8_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT16:
		TemplatedFullScanLoop<int16_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT32:
		TemplatedFullScanLoop<int32_t>(rows, col, count, col_offset, col_no);
		break;
	case PhysicalType::INT64:
		TemplatedFullScanLoop<int64_t>(rows, col, count, col_offset, col_no);
		break;
	default:
		throw NotImplementedException("Unimplemented type for RowOperations::FullScanColumn");
	}
}

} // namespace duckdb




namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

template <class T>
static void TemplatedHeapGather(Vector &v, const idx_t count, const SelectionVector &sel, data_ptr_t *key_locations) {
	auto target = FlatVector::GetData<T>(v);

	for (idx_t i = 0; i < count; ++i) {
		const auto col_idx = sel.get_index(i);
		target[col_idx] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
	}
}

static void HeapGatherStringVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);
	auto target = FlatVector::GetData<string_t>(v);

	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		auto len = Load<uint32_t>(key_locations[i]);
		key_locations[i] += sizeof(uint32_t);
		target[col_idx] = StringVector::AddStringOrBlob(v, string_t((const char *)key_locations[i], len));
		key_locations[i] += len;
	}
}

static void HeapGatherStructVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	// struct must have a validitymask for its fields
	auto &child_types = StructType::GetChildTypes(v.GetType());
	const idx_t struct_validitymask_size = (child_types.size() + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < vcount; i++) {
		// use key_locations as the validitymask, and create struct_key_locations
		struct_validitymask_locations[i] = key_locations[i];
		key_locations[i] += struct_validitymask_size;
	}

	// now deserialize into the struct vectors
	auto &children = StructVector::GetEntries(v);
	for (idx_t i = 0; i < child_types.size(); i++) {
		RowOperations::HeapGather(*children[i], vcount, sel, i, key_locations, struct_validitymask_locations);
	}
}

static void HeapGatherListVector(Vector &v, const idx_t vcount, const SelectionVector &sel, data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);

	auto child_type = ListType::GetChildType(v.GetType());
	auto list_data = ListVector::GetData(v);
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	uint64_t entry_offset = ListVector::GetListSize(v);
	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		// read list length
		auto entry_remaining = Load<uint64_t>(key_locations[i]);
		key_locations[i] += sizeof(uint64_t);
		// set list entry attributes
		list_data[col_idx].length = entry_remaining;
		list_data[col_idx].offset = entry_offset;
		// skip over the validity mask
		data_ptr_t validitymask_location = key_locations[i];
		idx_t offset_in_byte = 0;
		key_locations[i] += (entry_remaining + 7) / 8;
		// entry sizes
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type.InternalType())) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += entry_remaining * sizeof(idx_t);
		}

		// now read the list data
		while (entry_remaining > 0) {
			auto next = MinValue(entry_remaining, (idx_t)STANDARD_VECTOR_SIZE);

			// initialize a new vector to append
			Vector append_vector(v.GetType());
			append_vector.SetVectorType(v.GetVectorType());

			auto &list_vec_to_append = ListVector::GetEntry(append_vector);

			// set validity
			//! Since we are constructing the vector, this will always be a flat vector.
			auto &append_validity = FlatVector::Validity(list_vec_to_append);
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				append_validity.Set(entry_idx, *(validitymask_location) & (1 << offset_in_byte));
				if (++offset_in_byte == 8) {
					validitymask_location++;
					offset_in_byte = 0;
				}
			}

			// compute entry sizes and set locations where the list entries are
			if (TypeIsConstantSize(child_type.InternalType())) {
				// constant size list entries
				const idx_t type_size = GetTypeIdSize(child_type.InternalType());
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += Load<idx_t>(var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now deserialize and add to listvector
			RowOperations::HeapGather(list_vec_to_append, next, *FlatVector::IncrementalSelectionVector(), 0,
			                          list_entry_locations, nullptr);
			ListVector::Append(v, list_vec_to_append, next);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

void RowOperations::HeapGather(Vector &v, const idx_t &vcount, const SelectionVector &sel, const idx_t &col_no,
                               data_ptr_t *key_locations, data_ptr_t *validitymask_locations) {
	v.SetVectorType(VectorType::FLAT_VECTOR);

	auto &validity = FlatVector::Validity(v);
	if (validitymask_locations) {
		// Precompute mask indexes
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

		for (idx_t i = 0; i < vcount; i++) {
			ValidityBytes row_mask(validitymask_locations[i]);
			const auto valid = row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);
			const auto col_idx = sel.get_index(i);
			validity.Set(col_idx, valid);
		}
	}

	auto type = v.GetType().InternalType();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapGather<int8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT16:
		TemplatedHeapGather<int16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT32:
		TemplatedHeapGather<int32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT64:
		TemplatedHeapGather<int64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapGather<uint8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapGather<uint16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapGather<uint32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapGather<uint64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT128:
		TemplatedHeapGather<hugeint_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapGather<float>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapGather<double>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapGather<interval_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::VARCHAR:
		HeapGatherStringVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::STRUCT:
		HeapGatherStructVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::LIST:
		HeapGatherListVector(v, vcount, sel, key_locations);
		break;
	default:
		throw NotImplementedException("Unimplemented deserialize from row-format");
	}
}

} // namespace duckdb




namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

static void ComputeStringEntrySizes(VectorData &vdata, idx_t entry_sizes[], const idx_t ser_count,
                                    const SelectionVector &sel, const idx_t offset) {
	auto strings = (string_t *)vdata.data;
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto str_idx = vdata.sel->get_index(idx) + offset;
		if (vdata.validity.RowIsValid(str_idx)) {
			entry_sizes[i] += sizeof(uint32_t) + strings[str_idx].GetSize();
		}
	}
}

static void ComputeStructEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                    const SelectionVector &sel, idx_t offset) {
	// obtain child vectors
	idx_t num_children;
	auto &children = StructVector::GetEntries(v);
	num_children = children.size();
	// add struct validitymask size
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	for (idx_t i = 0; i < ser_count; i++) {
		entry_sizes[i] += struct_validitymask_size;
	}
	// compute size of child vectors
	for (auto &struct_vector : children) {
		RowOperations::ComputeEntrySizes(*struct_vector, entry_sizes, vcount, ser_count, sel, offset);
	}
}

static void ComputeListEntrySizes(Vector &v, VectorData &vdata, idx_t entry_sizes[], idx_t ser_count,
                                  const SelectionVector &sel, idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx) + offset;
		if (vdata.validity.RowIsValid(source_idx)) {
			auto list_entry = list_data[source_idx];

			// make room for list length, list validitymask
			entry_sizes[i] += sizeof(list_entry.length);
			entry_sizes[i] += (list_entry.length + 7) / 8;

			// serialize size of each entry (if non-constant size)
			if (!TypeIsConstantSize(ListType::GetChildType(v.GetType()).InternalType())) {
				entry_sizes[i] += list_entry.length * sizeof(list_entry.length);
			}

			// compute size of each the elements in list_entry and sum them
			auto entry_remaining = list_entry.length;
			auto entry_offset = list_entry.offset;
			while (entry_remaining > 0) {
				// the list entry can span multiple vectors
				auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

				// compute and add to the total
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t list_idx = 0; list_idx < next; list_idx++) {
					entry_sizes[i] += list_entry_sizes[list_idx];
				}

				// update for next iteration
				entry_remaining -= next;
				entry_offset += next;
			}
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, VectorData &vdata, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                      const SelectionVector &sel, idx_t offset) {
	const auto physical_type = v.GetType().InternalType();
	if (TypeIsConstantSize(physical_type)) {
		const auto type_size = GetTypeIdSize(physical_type);
		for (idx_t i = 0; i < ser_count; i++) {
			entry_sizes[i] += type_size;
		}
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR:
			ComputeStringEntrySizes(vdata, entry_sizes, ser_count, sel, offset);
			break;
		case PhysicalType::STRUCT:
			ComputeStructEntrySizes(v, entry_sizes, vcount, ser_count, sel, offset);
			break;
		case PhysicalType::LIST:
			ComputeListEntrySizes(v, vdata, entry_sizes, ser_count, sel, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Column with variable size type %s cannot be serialized to row-format",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                      const SelectionVector &sel, idx_t offset) {
	VectorData vdata;
	v.Orrify(vcount, vdata);
	ComputeEntrySizes(v, vdata, entry_sizes, vcount, ser_count, sel, offset);
}

template <class T>
static void TemplatedHeapScatter(VectorData &vdata, const SelectionVector &sel, idx_t count, idx_t col_idx,
                                 data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	auto source = (T *)vdata.data;
	if (!validitymask_locations) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);
		}
	} else {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
		const auto bit = ~(1UL << idx_in_entry);
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], (data_ptr_t)target);
			key_locations[i] += sizeof(T);

			// set the validitymask
			if (!vdata.validity.RowIsValid(source_idx)) {
				*(validitymask_locations[i] + entry_idx) &= bit;
			}
		}
	}
}

static void HeapScatterStringVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	auto strings = (string_t *)vdata.data;
	if (!validitymask_locations) {
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(string_entry.GetSize(), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetDataUnsafe(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			}
		}
	} else {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
		const auto bit = ~(1UL << idx_in_entry);
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(string_entry.GetSize(), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetDataUnsafe(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			} else {
				// set the validitymask
				*(validitymask_locations[i] + entry_idx) &= bit;
			}
		}
	}
}

static void HeapScatterStructVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                    data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	auto &children = StructVector::GetEntries(v);
	idx_t num_children = children.size();

	// the whole struct itself can be NULL
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	const auto bit = ~(1UL << idx_in_entry);

	// struct must have a validitymask for its fields
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		// initialize the struct validity mask
		struct_validitymask_locations[i] = key_locations[i];
		memset(struct_validitymask_locations[i], -1, struct_validitymask_size);
		key_locations[i] += struct_validitymask_size;

		// set whether the whole struct is null
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx) + offset;
		if (validitymask_locations && !vdata.validity.RowIsValid(source_idx)) {
			*(validitymask_locations[i] + entry_idx) &= bit;
		}
	}

	// now serialize the struct vectors
	for (idx_t i = 0; i < children.size(); i++) {
		auto &struct_vector = *children[i];
		RowOperations::HeapScatter(struct_vector, vcount, sel, ser_count, i, key_locations,
		                           struct_validitymask_locations, offset);
	}
}

static void HeapScatterListVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_no,
                                  data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	VectorData vdata;
	v.Orrify(vcount, vdata);

	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto list_data = ListVector::GetData(v);

	auto &child_vector = ListVector::GetEntry(v);

	VectorData list_vdata;
	child_vector.Orrify(ListVector::GetListSize(v), list_vdata);
	auto child_type = ListType::GetChildType(v.GetType()).InternalType();

	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx) + offset;
		if (!vdata.validity.RowIsValid(source_idx)) {
			if (validitymask_locations) {
				// set the row validitymask for this column to invalid
				ValidityBytes row_mask(validitymask_locations[i]);
				row_mask.SetInvalidUnsafe(entry_idx, idx_in_entry);
			}
			continue;
		}
		auto list_entry = list_data[source_idx];

		// store list length
		Store<uint64_t>(list_entry.length, key_locations[i]);
		key_locations[i] += sizeof(list_entry.length);

		// make room for the validitymask
		data_ptr_t list_validitymask_location = key_locations[i];
		idx_t entry_offset_in_byte = 0;
		idx_t validitymask_size = (list_entry.length + 7) / 8;
		memset(list_validitymask_location, -1, validitymask_size);
		key_locations[i] += validitymask_size;

		// serialize size of each entry (if non-constant size)
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type)) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += list_entry.length * sizeof(idx_t);
		}

		auto entry_remaining = list_entry.length;
		auto entry_offset = list_entry.offset;
		while (entry_remaining > 0) {
			// the list entry can span multiple vectors
			auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

			// serialize list validity
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				auto list_idx = list_vdata.sel->get_index(entry_idx) + entry_offset;
				if (!list_vdata.validity.RowIsValid(list_idx)) {
					*(list_validitymask_location) &= ~(1UL << entry_offset_in_byte);
				}
				if (++entry_offset_in_byte == 8) {
					list_validitymask_location++;
					entry_offset_in_byte = 0;
				}
			}

			if (TypeIsConstantSize(child_type)) {
				// constant size list entries: set list entry locations
				const idx_t type_size = GetTypeIdSize(child_type);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries: compute entry sizes and set list entry locations
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += list_entry_sizes[entry_idx];
					Store<idx_t>(list_entry_sizes[entry_idx], var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now serialize to the locations
			RowOperations::HeapScatter(child_vector, ListVector::GetListSize(v),
			                           *FlatVector::IncrementalSelectionVector(), next, 0, list_entry_locations,
			                           nullptr, entry_offset);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

void RowOperations::HeapScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count, idx_t col_idx,
                                data_ptr_t *key_locations, data_ptr_t *validitymask_locations, idx_t offset) {
	if (TypeIsConstantSize(v.GetType().InternalType())) {
		VectorData vdata;
		v.Orrify(vcount, vdata);
		RowOperations::HeapScatterVData(vdata, v.GetType().InternalType(), sel, ser_count, col_idx, key_locations,
		                                validitymask_locations, offset);
	} else {
		switch (v.GetType().InternalType()) {
		case PhysicalType::VARCHAR:
			HeapScatterStringVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		case PhysicalType::STRUCT:
			HeapScatterStructVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		case PhysicalType::LIST:
			HeapScatterListVector(v, vcount, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Serialization of variable length vector with type %s",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::HeapScatterVData(VectorData &vdata, PhysicalType type, const SelectionVector &sel, idx_t ser_count,
                                     idx_t col_idx, data_ptr_t *key_locations, data_ptr_t *validitymask_locations,
                                     idx_t offset) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapScatter<int8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT16:
		TemplatedHeapScatter<int16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT32:
		TemplatedHeapScatter<int32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT64:
		TemplatedHeapScatter<int64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapScatter<uint8_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapScatter<uint16_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapScatter<uint32_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapScatter<uint64_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INT128:
		TemplatedHeapScatter<hugeint_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapScatter<float>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapScatter<double>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapScatter<interval_t>(vdata, sel, ser_count, col_idx, key_locations, validitymask_locations, offset);
		break;
	default:
		throw NotImplementedException("FIXME: Serialize to of constant type column to row-format");
	}
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// row_match.cpp
// Description: This file contains the implementation of the match operators
//===--------------------------------------------------------------------===//







namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;
using Predicates = RowOperations::Predicates;

template <typename OP>
static idx_t SelectComparison(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                              SelectionVector *true_sel, SelectionVector *false_sel) {
	throw NotImplementedException("Unsupported nested comparison operand for RowOperations::Match");
}

template <>
idx_t SelectComparison<Equals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                               SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<NotEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                  SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::NestedNotEquals(left, right, sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                    SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<GreaterThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                          SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctGreaterThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThan>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThan(left, right, &sel, count, true_sel, false_sel);
}

template <>
idx_t SelectComparison<LessThanEquals>(Vector &left, Vector &right, const SelectionVector &sel, idx_t count,
                                       SelectionVector *true_sel, SelectionVector *false_sel) {
	return VectorOperations::DistinctLessThanEquals(left, right, &sel, count, true_sel, false_sel);
}

template <class T, class OP, bool NO_MATCH_SEL>
static void TemplatedMatchType(VectorData &col, Vector &rows, SelectionVector &sel, idx_t &count, idx_t col_offset,
                               idx_t col_no, SelectionVector *no_match, idx_t &no_match_count) {
	// Precompute row_mask indexes
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_no, entry_idx, idx_in_entry);

	auto data = (T *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	idx_t match_count = 0;
	if (!col.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);

			auto row = ptrs[idx];
			ValidityBytes row_mask(row);
			auto isnull = !row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);

			auto col_idx = col.sel->get_index(idx);
			if (!col.validity.RowIsValid(col_idx)) {
				if (isnull) {
					// match: move to next value to compare
					sel.set_index(match_count++, idx);
				} else {
					if (NO_MATCH_SEL) {
						no_match->set_index(no_match_count++, idx);
					}
				}
			} else {
				auto value = Load<T>(row + col_offset);
				if (!isnull && OP::template Operation<T>(data[col_idx], value)) {
					sel.set_index(match_count++, idx);
				} else {
					if (NO_MATCH_SEL) {
						no_match->set_index(no_match_count++, idx);
					}
				}
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);

			auto row = ptrs[idx];
			ValidityBytes row_mask(row);
			auto isnull = !row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry);

			auto col_idx = col.sel->get_index(idx);
			auto value = Load<T>(row + col_offset);
			if (!isnull && OP::template Operation<T>(data[col_idx], value)) {
				sel.set_index(match_count++, idx);
			} else {
				if (NO_MATCH_SEL) {
					no_match->set_index(no_match_count++, idx);
				}
			}
		}
	}
	count = match_count;
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchNested(Vector &col, Vector &rows, SelectionVector &sel, idx_t &count, const idx_t col_offset,
                                 const idx_t col_no, SelectionVector *no_match, idx_t &no_match_count) {
	// Gather a dense Vector containing the column values being matched
	Vector key(col.GetType());
	RowOperations::Gather(rows, sel, key, *FlatVector::IncrementalSelectionVector(), count, col_offset, col_no);

	// Densify the input column
	Vector sliced(col, sel, count);

	if (NO_MATCH_SEL) {
		SelectionVector no_match_sel_offset(no_match->data() + no_match_count);
		auto match_count = SelectComparison<OP>(sliced, key, sel, count, &sel, &no_match_sel_offset);
		no_match_count += count - match_count;
		count = match_count;
	} else {
		count = SelectComparison<OP>(sliced, key, sel, count, &sel, nullptr);
	}
}

template <class OP, bool NO_MATCH_SEL>
static void TemplatedMatchOp(Vector &vec, VectorData &col, const RowLayout &layout, Vector &rows, SelectionVector &sel,
                             idx_t &count, idx_t col_no, SelectionVector *no_match, idx_t &no_match_count) {
	if (count == 0) {
		return;
	}
	auto col_offset = layout.GetOffsets()[col_no];
	switch (layout.GetTypes()[col_no].InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedMatchType<int8_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                             no_match_count);
		break;
	case PhysicalType::INT16:
		TemplatedMatchType<int16_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::INT32:
		TemplatedMatchType<int32_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::INT64:
		TemplatedMatchType<int64_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::UINT8:
		TemplatedMatchType<uint8_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                              no_match_count);
		break;
	case PhysicalType::UINT16:
		TemplatedMatchType<uint16_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::UINT32:
		TemplatedMatchType<uint32_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::UINT64:
		TemplatedMatchType<uint64_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::INT128:
		TemplatedMatchType<hugeint_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                                no_match_count);
		break;
	case PhysicalType::FLOAT:
		TemplatedMatchType<float, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                            no_match_count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedMatchType<double, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                             no_match_count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedMatchType<interval_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                                 no_match_count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedMatchType<string_t, OP, NO_MATCH_SEL>(col, rows, sel, count, col_offset, col_no, no_match,
		                                               no_match_count);
		break;
	case PhysicalType::LIST:
	case PhysicalType::MAP:
	case PhysicalType::STRUCT:
		TemplatedMatchNested<OP, NO_MATCH_SEL>(vec, rows, sel, count, col_offset, col_no, no_match, no_match_count);
		break;
	default:
		throw InternalException("Unsupported column type for RowOperations::Match");
	}
}

template <bool NO_MATCH_SEL>
static void TemplatedMatch(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                           const Predicates &predicates, SelectionVector &sel, idx_t &count, SelectionVector *no_match,
                           idx_t &no_match_count) {
	for (idx_t col_no = 0; col_no < predicates.size(); ++col_no) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		switch (predicates[col_no]) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			TemplatedMatchOp<Equals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                       no_match_count);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			TemplatedMatchOp<NotEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                          no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			TemplatedMatchOp<GreaterThan, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                            no_match_count);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			TemplatedMatchOp<GreaterThanEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                                  no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			TemplatedMatchOp<LessThan, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                         no_match_count);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			TemplatedMatchOp<LessThanEquals, NO_MATCH_SEL>(vec, col, layout, rows, sel, count, col_no, no_match,
			                                               no_match_count);
			break;
		default:
			throw InternalException("Unsupported comparison type for RowOperations::Match");
		}
	}
}

idx_t RowOperations::Match(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                           const Predicates &predicates, SelectionVector &sel, idx_t count, SelectionVector *no_match,
                           idx_t &no_match_count) {
	if (no_match) {
		TemplatedMatch<true>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	} else {
		TemplatedMatch<false>(columns, col_data, layout, rows, predicates, sel, count, no_match, no_match_count);
	}

	return count;
}

} // namespace duckdb





namespace duckdb {

template <class T>
void TemplatedRadixScatter(VectorData &vdata, const SelectionVector &sel, idx_t add_count, data_ptr_t *key_locations,
                           const bool desc, const bool has_null, const bool nulls_first, const bool is_little_endian,
                           const idx_t offset) {
	auto source = (T *)vdata.data;
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeData<T>(key_locations[i] + 1, source[source_idx], is_little_endian);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < sizeof(T) + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', sizeof(T));
			}
			key_locations[i] += sizeof(T) + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeData<T>(key_locations[i], source[source_idx], is_little_endian);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < sizeof(T); s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += sizeof(T);
		}
	}
}

void RadixScatterStringVector(VectorData &vdata, const SelectionVector &sel, idx_t add_count, data_ptr_t *key_locations,
                              const bool desc, const bool has_null, const bool nulls_first, const idx_t prefix_len,
                              idx_t offset) {
	auto source = (string_t *)vdata.data;
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeStringDataPrefix(key_locations[i] + 1, source[source_idx], prefix_len);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < prefix_len + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', prefix_len);
			}
			key_locations[i] += prefix_len + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeStringDataPrefix(key_locations[i], source[source_idx], prefix_len);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < prefix_len; s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += prefix_len;
		}
	}
}

void RadixScatterListVector(Vector &v, VectorData &vdata, const SelectionVector &sel, idx_t add_count,
                            data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                            const idx_t prefix_len, const idx_t width, const idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	auto list_size = ListVector::GetListSize(v);

	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			data_ptr_t key_location = key_locations[i] + 1;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				key_locations[i]++;
				auto &list_entry = list_data[source_idx];
				if (list_entry.length > 0) {
					// denote that the list is not empty with a 1
					key_locations[i][0] = 1;
					key_locations[i]++;
					RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
					                            key_locations + i, false, true, false, prefix_len, width - 1,
					                            list_entry.offset);
				} else {
					// denote that the list is empty with a 0
					key_locations[i][0] = 0;
					key_locations[i]++;
					memset(key_locations[i], '\0', width - 2);
				}
				// invert bits if desc
				if (desc) {
					for (idx_t s = 0; s < width - 1; s++) {
						*(key_location + s) = ~*(key_location + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', width - 1);
				key_locations[i] += width;
			}
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			auto &list_entry = list_data[source_idx];
			data_ptr_t key_location = key_locations[i];
			if (list_entry.length > 0) {
				// denote that the list is not empty with a 1
				key_locations[i][0] = 1;
				key_locations[i]++;
				RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
				                            key_locations + i, false, true, false, prefix_len, width - 1,
				                            list_entry.offset);
			} else {
				// denote that the list is empty with a 0
				key_locations[i][0] = 0;
				key_locations[i]++;
				memset(key_locations[i], '\0', width - 1);
			}
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < width; s++) {
					*(key_location + s) = ~*(key_location + s);
				}
			}
		}
	}
}

void RadixScatterStructVector(Vector &v, VectorData &vdata, idx_t vcount, const SelectionVector &sel, idx_t add_count,
                              data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                              const idx_t prefix_len, idx_t width, const idx_t offset) {
	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
			} else {
				key_locations[i][0] = invalid;
			}
			key_locations[i]++;
		}
		width--;
	}
	// serialize the struct
	auto &child_vector = *StructVector::GetEntries(v)[0];
	RowOperations::RadixScatter(child_vector, vcount, *FlatVector::IncrementalSelectionVector(), add_count,
	                            key_locations, false, true, false, prefix_len, width, offset);
	// invert bits if desc
	if (desc) {
		for (idx_t i = 0; i < add_count; i++) {
			for (idx_t s = 0; s < width; s++) {
				*(key_locations[i] - width + s) = ~*(key_locations[i] - width + s);
			}
		}
	}
}

void RowOperations::RadixScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                 data_ptr_t *key_locations, bool desc, bool has_null, bool nulls_first,
                                 idx_t prefix_len, idx_t width, idx_t offset) {
	auto is_little_endian = Radix::IsLittleEndian();

	VectorData vdata;
	v.Orrify(vcount, vdata);
	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedRadixScatter<int8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                              is_little_endian, offset);
		break;
	case PhysicalType::INT16:
		TemplatedRadixScatter<int16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                               is_little_endian, offset);
		break;
	case PhysicalType::INT32:
		TemplatedRadixScatter<int32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                               is_little_endian, offset);
		break;
	case PhysicalType::INT64:
		TemplatedRadixScatter<int64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                               is_little_endian, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedRadixScatter<uint8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                               is_little_endian, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedRadixScatter<uint16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                                is_little_endian, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedRadixScatter<uint32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                                is_little_endian, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedRadixScatter<uint64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                                is_little_endian, offset);
		break;
	case PhysicalType::INT128:
		TemplatedRadixScatter<hugeint_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                                 is_little_endian, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedRadixScatter<float>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                             is_little_endian, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedRadixScatter<double>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                              is_little_endian, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedRadixScatter<interval_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                                  is_little_endian, offset);
		break;
	case PhysicalType::VARCHAR:
		RadixScatterStringVector(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, offset);
		break;
	case PhysicalType::LIST:
		RadixScatterListVector(v, vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, width,
		                       offset);
		break;
	case PhysicalType::STRUCT:
		RadixScatterStructVector(v, vdata, vcount, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                         prefix_len, width, offset);
		break;
	default:
		throw NotImplementedException("Cannot ORDER BY column with type %s", v.GetType().ToString());
	}
}

} // namespace duckdb
//===--------------------------------------------------------------------===//
// row_scatter.cpp
// Description: This file contains the implementation of the row scattering
//              operators
//===--------------------------------------------------------------------===//










namespace duckdb {

using ValidityBytes = RowLayout::ValidityBytes;

template <class T>
static void TemplatedScatter(VectorData &col, Vector &rows, const SelectionVector &sel, const idx_t count,
                             const idx_t col_offset, const idx_t col_no) {
	auto data = (T *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	if (!col.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			auto isnull = !col.validity.RowIsValid(col_idx);
			T store_value = isnull ? NullValue<T>() : data[col_idx];
			Store<T>(store_value, row + col_offset);
			if (isnull) {
				ValidityBytes col_mask(ptrs[idx]);
				col_mask.SetInvalidUnsafe(col_no);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto col_idx = col.sel->get_index(idx);
			auto row = ptrs[idx];

			Store<T>(data[col_idx], row + col_offset);
		}
	}
}

static void ComputeStringEntrySizes(const VectorData &col, idx_t entry_sizes[], const SelectionVector &sel,
                                    const idx_t count, const idx_t offset = 0) {
	auto data = (const string_t *)col.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx) + offset;
		const auto &str = data[col_idx];
		if (col.validity.RowIsValid(col_idx) && !str.IsInlined()) {
			entry_sizes[i] += str.GetSize();
		}
	}
}

static void ScatterStringVector(VectorData &col, Vector &rows, data_ptr_t str_locations[], const SelectionVector &sel,
                                const idx_t count, const idx_t col_offset, const idx_t col_no) {
	auto string_data = (string_t *)col.data;
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);

	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto col_idx = col.sel->get_index(idx);
		auto row = ptrs[idx];
		if (!col.validity.RowIsValid(col_idx)) {
			ValidityBytes col_mask(row);
			col_mask.SetInvalidUnsafe(col_no);
			Store<string_t>(NullValue<string_t>(), row + col_offset);
		} else if (string_data[col_idx].IsInlined()) {
			Store<string_t>(string_data[col_idx], row + col_offset);
		} else {
			const auto &str = string_data[col_idx];
			string_t inserted((const char *)str_locations[i], str.GetSize());
			memcpy(inserted.GetDataWriteable(), str.GetDataUnsafe(), str.GetSize());
			str_locations[i] += str.GetSize();
			inserted.Finalize();
			Store<string_t>(inserted, row + col_offset);
		}
	}
}

static void ScatterNestedVector(Vector &vec, VectorData &col, Vector &rows, data_ptr_t data_locations[],
                                const SelectionVector &sel, const idx_t count, const idx_t col_offset,
                                const idx_t col_no, const idx_t vcount) {
	// Store pointers to the data in the row
	// Do this first because SerializeVector destroys the locations
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	data_ptr_t validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto row = ptrs[idx];
		validitymask_locations[i] = row;

		Store<data_ptr_t>(data_locations[i], row + col_offset);
	}

	// Serialise the data
	RowOperations::HeapScatter(vec, vcount, sel, count, col_no, data_locations, validitymask_locations);
}

void RowOperations::Scatter(DataChunk &columns, VectorData col_data[], const RowLayout &layout, Vector &rows,
                            RowDataCollection &string_heap, const SelectionVector &sel, idx_t count) {
	if (count == 0) {
		return;
	}

	// Set the validity mask for each row before inserting data
	auto ptrs = FlatVector::GetData<data_ptr_t>(rows);
	for (idx_t i = 0; i < count; ++i) {
		auto row_idx = sel.get_index(i);
		auto row = ptrs[row_idx];
		ValidityBytes(row).SetAllValid(layout.ColumnCount());
	}

	const auto vcount = columns.size();
	auto &offsets = layout.GetOffsets();
	auto &types = layout.GetTypes();

	// Compute the entry size of the variable size columns
	vector<BufferHandle> handles;
	data_ptr_t data_locations[STANDARD_VECTOR_SIZE];
	if (!layout.AllConstant()) {
		idx_t entry_sizes[STANDARD_VECTOR_SIZE];
		std::fill_n(entry_sizes, count, sizeof(uint32_t));
		for (idx_t col_no = 0; col_no < types.size(); col_no++) {
			if (TypeIsConstantSize(types[col_no].InternalType())) {
				continue;
			}

			auto &vec = columns.data[col_no];
			auto &col = col_data[col_no];
			switch (types[col_no].InternalType()) {
			case PhysicalType::VARCHAR:
				ComputeStringEntrySizes(col, entry_sizes, sel, count);
				break;
			case PhysicalType::LIST:
			case PhysicalType::MAP:
			case PhysicalType::STRUCT:
				RowOperations::ComputeEntrySizes(vec, col, entry_sizes, vcount, count, sel);
				break;
			default:
				throw InternalException("Unsupported type for RowOperations::Scatter");
			}
		}

		// Build out the buffer space
		string_heap.Build(count, data_locations, entry_sizes);

		// Serialize information that is needed for swizzling if the computation goes out-of-core
		const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < count; i++) {
			auto row_idx = sel.get_index(i);
			auto row = ptrs[row_idx];
			// Pointer to this row in the heap block
			Store<data_ptr_t>(data_locations[i], row + heap_pointer_offset);
			// Row size is stored in the heap in front of each row
			Store<uint32_t>(entry_sizes[i], data_locations[i]);
			data_locations[i] += sizeof(uint32_t);
		}
	}

	for (idx_t col_no = 0; col_no < types.size(); col_no++) {
		auto &vec = columns.data[col_no];
		auto &col = col_data[col_no];
		auto col_offset = offsets[col_no];

		switch (types[col_no].InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			TemplatedScatter<int8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT16:
			TemplatedScatter<int16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT32:
			TemplatedScatter<int32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT64:
			TemplatedScatter<int64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT8:
			TemplatedScatter<uint8_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT16:
			TemplatedScatter<uint16_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT32:
			TemplatedScatter<uint32_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::UINT64:
			TemplatedScatter<uint64_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INT128:
			TemplatedScatter<hugeint_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::FLOAT:
			TemplatedScatter<float>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::DOUBLE:
			TemplatedScatter<double>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::INTERVAL:
			TemplatedScatter<interval_t>(col, rows, sel, count, col_offset, col_no);
			break;
		case PhysicalType::VARCHAR:
			ScatterStringVector(col, rows, data_locations, sel, count, col_offset, col_no);
			break;
		case PhysicalType::LIST:
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			ScatterNestedVector(vec, col, rows, data_locations, sel, count, col_offset, col_no, vcount);
			break;
		default:
			throw InternalException("Unsupported type for RowOperations::Scatter");
		}
	}
}

} // namespace duckdb


#include <cstring>

namespace duckdb {

BufferedDeserializer::BufferedDeserializer(data_ptr_t ptr, idx_t data_size) : ptr(ptr), endptr(ptr + data_size) {
}

BufferedDeserializer::BufferedDeserializer(BufferedSerializer &serializer)
    : BufferedDeserializer(serializer.data, serializer.maximum_size) {
}

void BufferedDeserializer::ReadData(data_ptr_t buffer, idx_t read_size) {
	if (ptr + read_size > endptr) {
		throw SerializationException("Failed to deserialize: not enough data in buffer to fulfill read request");
	}
	memcpy(buffer, ptr, read_size);
	ptr += read_size;
}

} // namespace duckdb




#include <cstring>
#include <algorithm>

namespace duckdb {

BufferedFileReader::BufferedFileReader(FileSystem &fs, const char *path, FileOpener *opener)
    : fs(fs), data(unique_ptr<data_t[]>(new data_t[FILE_BUFFER_SIZE])), offset(0), read_data(0), total_read(0) {
	handle =
	    fs.OpenFile(path, FileFlags::FILE_FLAGS_READ, FileLockType::READ_LOCK, FileSystem::DEFAULT_COMPRESSION, opener);
	file_size = fs.GetFileSize(*handle);
}

void BufferedFileReader::ReadData(data_ptr_t target_buffer, uint64_t read_size) {
	// first copy anything we can from the buffer
	data_ptr_t end_ptr = target_buffer + read_size;
	while (true) {
		idx_t to_read = MinValue<idx_t>(end_ptr - target_buffer, read_data - offset);
		if (to_read > 0) {
			memcpy(target_buffer, data.get() + offset, to_read);
			offset += to_read;
			target_buffer += to_read;
		}
		if (target_buffer < end_ptr) {
			D_ASSERT(offset == read_data);
			total_read += read_data;
			// did not finish reading yet but exhausted buffer
			// read data into buffer
			offset = 0;
			read_data = fs.Read(*handle, data.get(), FILE_BUFFER_SIZE);
			if (read_data == 0) {
				throw SerializationException("not enough data in file to deserialize result");
			}
		} else {
			return;
		}
	}
}

bool BufferedFileReader::Finished() {
	return total_read + offset == file_size;
}

} // namespace duckdb



#include <cstring>

namespace duckdb {

// Remove this when we switch C++17: https://stackoverflow.com/a/53350948
constexpr uint8_t BufferedFileWriter::DEFAULT_OPEN_FLAGS;

BufferedFileWriter::BufferedFileWriter(FileSystem &fs, const string &path_p, uint8_t open_flags, FileOpener *opener)
    : fs(fs), path(path_p), data(unique_ptr<data_t[]>(new data_t[FILE_BUFFER_SIZE])), offset(0), total_written(0) {
	handle = fs.OpenFile(path, open_flags, FileLockType::WRITE_LOCK, FileSystem::DEFAULT_COMPRESSION, opener);
}

int64_t BufferedFileWriter::GetFileSize() {
	return fs.GetFileSize(*handle);
}

idx_t BufferedFileWriter::GetTotalWritten() {
	return total_written + offset;
}

void BufferedFileWriter::WriteData(const_data_ptr_t buffer, uint64_t write_size) {
	// first copy anything we can from the buffer
	const_data_ptr_t end_ptr = buffer + write_size;
	while (buffer < end_ptr) {
		idx_t to_write = MinValue<idx_t>((end_ptr - buffer), FILE_BUFFER_SIZE - offset);
		D_ASSERT(to_write > 0);
		memcpy(data.get() + offset, buffer, to_write);
		offset += to_write;
		buffer += to_write;
		if (offset == FILE_BUFFER_SIZE) {
			Flush();
		}
	}
}

void BufferedFileWriter::Flush() {
	if (offset == 0) {
		return;
	}
	fs.Write(*handle, data.get(), offset);
	total_written += offset;
	offset = 0;
}

void BufferedFileWriter::Sync() {
	Flush();
	handle->Sync();
}

void BufferedFileWriter::Truncate(int64_t size) {
	// truncate the physical file on disk
	handle->Truncate(size);
	// reset anything written in the buffer
	offset = 0;
}

} // namespace duckdb


#include <cstring>

namespace duckdb {

BufferedSerializer::BufferedSerializer(idx_t maximum_size)
    : BufferedSerializer(unique_ptr<data_t[]>(new data_t[maximum_size]), maximum_size) {
}

BufferedSerializer::BufferedSerializer(unique_ptr<data_t[]> data, idx_t size) : maximum_size(size), data(data.get()) {
	blob.size = 0;
	blob.data = move(data);
}

BufferedSerializer::BufferedSerializer(data_ptr_t data, idx_t size) : maximum_size(size), data(data) {
	blob.size = 0;
}

void BufferedSerializer::WriteData(const_data_ptr_t buffer, idx_t write_size) {
	if (blob.size + write_size >= maximum_size) {
		do {
			maximum_size *= 2;
		} while (blob.size + write_size > maximum_size);
		auto new_data = new data_t[maximum_size];
		memcpy(new_data, data, blob.size);
		data = new_data;
		blob.data = unique_ptr<data_t[]>(new_data);
	}

	memcpy(data + blob.size, buffer, write_size);
	blob.size += write_size;
}

} // namespace duckdb


namespace duckdb {

template <>
string Deserializer::Read() {
	uint32_t size = Read<uint32_t>();
	if (size == 0) {
		return string();
	}
	auto buffer = unique_ptr<data_t[]>(new data_t[size]);
	ReadData(buffer.get(), size);
	return string((char *)buffer.get(), size);
}

void Deserializer::ReadStringVector(vector<string> &list) {
	uint32_t sz = Read<uint32_t>();
	list.resize(sz);
	for (idx_t i = 0; i < sz; i++) {
		list[i] = Read<string>();
	}
}

} // namespace duckdb





namespace duckdb {

bool Comparators::TieIsBreakable(const idx_t &col_idx, const data_ptr_t row_ptr, const RowLayout &row_layout) {
	// Check if the blob is NULL
	ValidityBytes row_mask(row_ptr);
	idx_t entry_idx;
	idx_t idx_in_entry;
	ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
	if (!row_mask.RowIsValid(row_mask.GetValidityEntry(entry_idx), idx_in_entry)) {
		// Can't break a NULL tie
		return false;
	}
	if (row_layout.GetTypes()[col_idx].InternalType() == PhysicalType::VARCHAR) {
		const auto &tie_col_offset = row_layout.GetOffsets()[col_idx];
		string_t tie_string = Load<string_t>(row_ptr + tie_col_offset);
		if (tie_string.GetSize() < string_t::INLINE_LENGTH) {
			// No need to break the tie - we already compared the full string
			return false;
		}
	}
	return true;
}

int Comparators::CompareTuple(const SBScanState &left, const SBScanState &right, const data_ptr_t &l_ptr,
                              const data_ptr_t &r_ptr, const SortLayout &sort_layout, const bool &external_sort) {
	// Compare the sorting columns one by one
	int comp_res = 0;
	data_ptr_t l_ptr_offset = l_ptr;
	data_ptr_t r_ptr_offset = r_ptr;
	for (idx_t col_idx = 0; col_idx < sort_layout.column_count; col_idx++) {
		comp_res = FastMemcmp(l_ptr_offset, r_ptr_offset, sort_layout.column_sizes[col_idx]);
		if (comp_res == 0 && !sort_layout.constant_size[col_idx]) {
			comp_res = BreakBlobTie(col_idx, left, right, sort_layout, external_sort);
		}
		if (comp_res != 0) {
			break;
		}
		l_ptr_offset += sort_layout.column_sizes[col_idx];
		r_ptr_offset += sort_layout.column_sizes[col_idx];
	}
	return comp_res;
}

int Comparators::CompareVal(const data_ptr_t l_ptr, const data_ptr_t r_ptr, const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR:
		return TemplatedCompareVal<string_t>(l_ptr, r_ptr);
	case PhysicalType::LIST:
	case PhysicalType::STRUCT: {
		auto l_nested_ptr = Load<data_ptr_t>(l_ptr);
		auto r_nested_ptr = Load<data_ptr_t>(r_ptr);
		return CompareValAndAdvance(l_nested_ptr, r_nested_ptr, type);
	}
	default:
		throw NotImplementedException("Unimplemented CompareVal for type %s", type.ToString());
	}
}

int Comparators::BreakBlobTie(const idx_t &tie_col, const SBScanState &left, const SBScanState &right,
                              const SortLayout &sort_layout, const bool &external) {
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	data_ptr_t l_data_ptr = left.DataPtr(*left.sb->blob_sorting_data);
	data_ptr_t r_data_ptr = right.DataPtr(*right.sb->blob_sorting_data);
	if (!TieIsBreakable(col_idx, l_data_ptr, sort_layout.blob_layout)) {
		// Quick check to see if ties can be broken
		return 0;
	}
	// Align the pointers
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	l_data_ptr += tie_col_offset;
	r_data_ptr += tie_col_offset;
	// Do the comparison
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &type = sort_layout.blob_layout.GetTypes()[col_idx];
	int result;
	if (external) {
		// Store heap pointers
		data_ptr_t l_heap_ptr = left.HeapPtr(*left.sb->blob_sorting_data);
		data_ptr_t r_heap_ptr = right.HeapPtr(*right.sb->blob_sorting_data);
		// Unswizzle offset to pointer
		UnswizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		UnswizzleSingleValue(r_data_ptr, r_heap_ptr, type);
		// Compare
		result = CompareVal(l_data_ptr, r_data_ptr, type);
		// Swizzle the pointers back to offsets
		SwizzleSingleValue(l_data_ptr, l_heap_ptr, type);
		SwizzleSingleValue(r_data_ptr, r_heap_ptr, type);
	} else {
		result = CompareVal(l_data_ptr, r_data_ptr, type);
	}
	return order * result;
}

template <class T>
int Comparators::TemplatedCompareVal(const data_ptr_t &left_ptr, const data_ptr_t &right_ptr) {
	const auto left_val = Load<T>(left_ptr);
	const auto right_val = Load<T>(right_ptr);
	if (Equals::Operation<T>(left_val, right_val)) {
		return 0;
	} else if (LessThan::Operation<T>(left_val, right_val)) {
		return -1;
	} else {
		return 1;
	}
}

int Comparators::CompareValAndAdvance(data_ptr_t &l_ptr, data_ptr_t &r_ptr, const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedCompareAndAdvance<int8_t>(l_ptr, r_ptr);
	case PhysicalType::INT16:
		return TemplatedCompareAndAdvance<int16_t>(l_ptr, r_ptr);
	case PhysicalType::INT32:
		return TemplatedCompareAndAdvance<int32_t>(l_ptr, r_ptr);
	case PhysicalType::INT64:
		return TemplatedCompareAndAdvance<int64_t>(l_ptr, r_ptr);
	case PhysicalType::UINT8:
		return TemplatedCompareAndAdvance<uint8_t>(l_ptr, r_ptr);
	case PhysicalType::UINT16:
		return TemplatedCompareAndAdvance<uint16_t>(l_ptr, r_ptr);
	case PhysicalType::UINT32:
		return TemplatedCompareAndAdvance<uint32_t>(l_ptr, r_ptr);
	case PhysicalType::UINT64:
		return TemplatedCompareAndAdvance<uint64_t>(l_ptr, r_ptr);
	case PhysicalType::INT128:
		return TemplatedCompareAndAdvance<hugeint_t>(l_ptr, r_ptr);
	case PhysicalType::FLOAT:
		return TemplatedCompareAndAdvance<float>(l_ptr, r_ptr);
	case PhysicalType::DOUBLE:
		return TemplatedCompareAndAdvance<double>(l_ptr, r_ptr);
	case PhysicalType::INTERVAL:
		return TemplatedCompareAndAdvance<interval_t>(l_ptr, r_ptr);
	case PhysicalType::VARCHAR:
		return CompareStringAndAdvance(l_ptr, r_ptr);
	case PhysicalType::LIST:
		return CompareListAndAdvance(l_ptr, r_ptr, ListType::GetChildType(type));
	case PhysicalType::STRUCT:
		return CompareStructAndAdvance(l_ptr, r_ptr, StructType::GetChildTypes(type));
	default:
		throw NotImplementedException("Unimplemented CompareValAndAdvance for type %s", type.ToString());
	}
}

template <class T>
int Comparators::TemplatedCompareAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr) {
	auto result = TemplatedCompareVal<T>(left_ptr, right_ptr);
	left_ptr += sizeof(T);
	right_ptr += sizeof(T);
	return result;
}

int Comparators::CompareStringAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr) {
	// Construct the string_t
	uint32_t left_string_size = Load<uint32_t>(left_ptr);
	uint32_t right_string_size = Load<uint32_t>(right_ptr);
	left_ptr += sizeof(uint32_t);
	right_ptr += sizeof(uint32_t);
	string_t left_val((const char *)left_ptr, left_string_size);
	string_t right_val((const char *)right_ptr, right_string_size);
	left_ptr += left_string_size;
	right_ptr += right_string_size;
	// Compare
	return TemplatedCompareVal<string_t>((data_ptr_t)&left_val, (data_ptr_t)&right_val);
}

int Comparators::CompareStructAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                         const child_list_t<LogicalType> &types) {
	idx_t count = types.size();
	// Load validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (count + 7) / 8;
	right_ptr += (count + 7) / 8;
	// Initialize variables
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	// Compare
	int comp_res = 0;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		auto &type = types[i].second;
		if ((left_valid && right_valid) || TypeIsConstantSize(type.InternalType())) {
			comp_res = CompareValAndAdvance(left_ptr, right_ptr, types[i].second);
		}
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

int Comparators::CompareListAndAdvance(data_ptr_t &left_ptr, data_ptr_t &right_ptr, const LogicalType &type) {
	// Load list lengths
	auto left_len = Load<idx_t>(left_ptr);
	auto right_len = Load<idx_t>(right_ptr);
	left_ptr += sizeof(idx_t);
	right_ptr += sizeof(idx_t);
	// Load list validity masks
	ValidityBytes left_validity(left_ptr);
	ValidityBytes right_validity(right_ptr);
	left_ptr += (left_len + 7) / 8;
	right_ptr += (right_len + 7) / 8;
	// Compare
	int comp_res = 0;
	idx_t count = MinValue(left_len, right_len);
	if (TypeIsConstantSize(type.InternalType())) {
		// Templated code for fixed-size types
		switch (type.InternalType()) {
		case PhysicalType::BOOL:
		case PhysicalType::INT8:
			comp_res = TemplatedCompareListLoop<int8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT16:
			comp_res = TemplatedCompareListLoop<int16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT32:
			comp_res = TemplatedCompareListLoop<int32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT64:
			comp_res = TemplatedCompareListLoop<int64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT8:
			comp_res = TemplatedCompareListLoop<uint8_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT16:
			comp_res = TemplatedCompareListLoop<uint16_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT32:
			comp_res = TemplatedCompareListLoop<uint32_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::UINT64:
			comp_res = TemplatedCompareListLoop<uint64_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INT128:
			comp_res = TemplatedCompareListLoop<hugeint_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::FLOAT:
			comp_res = TemplatedCompareListLoop<float>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::DOUBLE:
			comp_res = TemplatedCompareListLoop<double>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		case PhysicalType::INTERVAL:
			comp_res = TemplatedCompareListLoop<interval_t>(left_ptr, right_ptr, left_validity, right_validity, count);
			break;
		default:
			throw NotImplementedException("CompareListAndAdvance for fixed-size type %s", type.ToString());
		}
	} else {
		// Variable-sized list entries
		bool left_valid;
		bool right_valid;
		idx_t entry_idx;
		idx_t idx_in_entry;
		// Size (in bytes) of all variable-sizes entries is stored before the entries begin,
		// to make deserialization easier. We need to skip over them
		left_ptr += left_len * sizeof(idx_t);
		right_ptr += right_len * sizeof(idx_t);
		for (idx_t i = 0; i < count; i++) {
			ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
			left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
			right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
			if (left_valid && right_valid) {
				switch (type.InternalType()) {
				case PhysicalType::LIST:
					comp_res = CompareListAndAdvance(left_ptr, right_ptr, ListType::GetChildType(type));
					break;
				case PhysicalType::VARCHAR:
					comp_res = CompareStringAndAdvance(left_ptr, right_ptr);
					break;
				case PhysicalType::STRUCT:
					comp_res = CompareStructAndAdvance(left_ptr, right_ptr, StructType::GetChildTypes(type));
					break;
				default:
					throw NotImplementedException("CompareListAndAdvance for variable-size type %s", type.ToString());
				}
			} else if (!left_valid && !right_valid) {
				comp_res = 0;
			} else if (left_valid) {
				comp_res = -1;
			} else {
				comp_res = 1;
			}
			if (comp_res != 0) {
				break;
			}
		}
	}
	// All values that we looped over were equal
	if (comp_res == 0 && left_len != right_len) {
		// Smaller lists first
		if (left_len < right_len) {
			comp_res = -1;
		} else {
			comp_res = 1;
		}
	}
	return comp_res;
}

template <class T>
int Comparators::TemplatedCompareListLoop(data_ptr_t &left_ptr, data_ptr_t &right_ptr,
                                          const ValidityBytes &left_validity, const ValidityBytes &right_validity,
                                          const idx_t &count) {
	int comp_res = 0;
	bool left_valid;
	bool right_valid;
	idx_t entry_idx;
	idx_t idx_in_entry;
	for (idx_t i = 0; i < count; i++) {
		ValidityBytes::GetEntryIndex(i, entry_idx, idx_in_entry);
		left_valid = left_validity.RowIsValid(left_validity.GetValidityEntry(entry_idx), idx_in_entry);
		right_valid = right_validity.RowIsValid(right_validity.GetValidityEntry(entry_idx), idx_in_entry);
		comp_res = TemplatedCompareAndAdvance<T>(left_ptr, right_ptr);
		if (!left_valid && !right_valid) {
			comp_res = 0;
		} else if (!left_valid) {
			comp_res = 1;
		} else if (!right_valid) {
			comp_res = -1;
		}
		if (comp_res != 0) {
			break;
		}
	}
	return comp_res;
}

void Comparators::UnswizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += sizeof(uint32_t) + string_t::PREFIX_LENGTH;
	}
	Store<data_ptr_t>(heap_ptr + Load<idx_t>(data_ptr), data_ptr);
}

void Comparators::SwizzleSingleValue(data_ptr_t data_ptr, const data_ptr_t &heap_ptr, const LogicalType &type) {
	if (type.InternalType() == PhysicalType::VARCHAR) {
		data_ptr += sizeof(uint32_t) + string_t::PREFIX_LENGTH;
	}
	Store<idx_t>(Load<data_ptr_t>(data_ptr) - heap_ptr, data_ptr);
}

} // namespace duckdb




namespace duckdb {

MergeSorter::MergeSorter(GlobalSortState &state, BufferManager &buffer_manager)
    : state(state), buffer_manager(buffer_manager), sort_layout(state.sort_layout) {
}

void MergeSorter::PerformInMergeRound() {
	while (true) {
		{
			lock_guard<mutex> pair_guard(state.lock);
			if (state.pair_idx == state.num_pairs) {
				break;
			}
			GetNextPartition();
		}
		MergePartition();
	}
}

void MergeSorter::MergePartition() {
	auto &left_block = *left->sb;
	auto &right_block = *right->sb;
#ifdef DEBUG
	D_ASSERT(left_block.radix_sorting_data.size() == left_block.payload_data->data_blocks.size());
	D_ASSERT(right_block.radix_sorting_data.size() == right_block.payload_data->data_blocks.size());
	if (!state.payload_layout.AllConstant() && state.external) {
		D_ASSERT(left_block.payload_data->data_blocks.size() == left_block.payload_data->heap_blocks.size());
		D_ASSERT(right_block.payload_data->data_blocks.size() == right_block.payload_data->heap_blocks.size());
	}
	if (!sort_layout.all_constant) {
		D_ASSERT(left_block.radix_sorting_data.size() == left_block.blob_sorting_data->data_blocks.size());
		D_ASSERT(right_block.radix_sorting_data.size() == right_block.blob_sorting_data->data_blocks.size());
		if (state.external) {
			D_ASSERT(left_block.blob_sorting_data->data_blocks.size() ==
			         left_block.blob_sorting_data->heap_blocks.size());
			D_ASSERT(right_block.blob_sorting_data->data_blocks.size() ==
			         right_block.blob_sorting_data->heap_blocks.size());
		}
	}
#endif
	// Set up the write block
	// Each merge task produces a SortedBlock with exactly state.block_capacity rows or less
	result->InitializeWrite();
	// Initialize arrays to store merge data
	bool left_smaller[STANDARD_VECTOR_SIZE];
	idx_t next_entry_sizes[STANDARD_VECTOR_SIZE];
	// Merge loop
#ifdef DEBUG
	auto l_count = left->Remaining();
	auto r_count = right->Remaining();
#endif
	while (true) {
		auto l_remaining = left->Remaining();
		auto r_remaining = right->Remaining();
		if (l_remaining + r_remaining == 0) {
			// Done
			break;
		}
		const idx_t next = MinValue(l_remaining + r_remaining, (idx_t)STANDARD_VECTOR_SIZE);
		if (l_remaining != 0 && r_remaining != 0) {
			// Compute the merge (not needed if one side is exhausted)
			ComputeMerge(next, left_smaller);
		}
		// Actually merge the data (radix, blob, and payload)
		MergeRadix(next, left_smaller);
		if (!sort_layout.all_constant) {
			MergeData(*result->blob_sorting_data, *left_block.blob_sorting_data, *right_block.blob_sorting_data, next,
			          left_smaller, next_entry_sizes, true);
			D_ASSERT(result->radix_sorting_data.size() == result->blob_sorting_data->data_blocks.size());
		}
		MergeData(*result->payload_data, *left_block.payload_data, *right_block.payload_data, next, left_smaller,
		          next_entry_sizes, false);
		D_ASSERT(result->radix_sorting_data.size() == result->payload_data->data_blocks.size());
	}
#ifdef DEBUG
	D_ASSERT(result->Count() == l_count + r_count);
#endif
}

void MergeSorter::GetNextPartition() {
	// Create result block
	state.sorted_blocks_temp[state.pair_idx].push_back(make_unique<SortedBlock>(buffer_manager, state));
	result = state.sorted_blocks_temp[state.pair_idx].back().get();
	// Determine which blocks must be merged
	auto &left_block = *state.sorted_blocks[state.pair_idx * 2];
	auto &right_block = *state.sorted_blocks[state.pair_idx * 2 + 1];
	const idx_t l_count = left_block.Count();
	const idx_t r_count = right_block.Count();
	// Initialize left and right reader
	left = make_unique<SBScanState>(buffer_manager, state);
	right = make_unique<SBScanState>(buffer_manager, state);
	// Compute the work that this thread must do using Merge Path
	idx_t l_end;
	idx_t r_end;
	if (state.l_start + state.r_start + state.block_capacity < l_count + r_count) {
		left->sb = state.sorted_blocks[state.pair_idx * 2].get();
		right->sb = state.sorted_blocks[state.pair_idx * 2 + 1].get();
		const idx_t intersection = state.l_start + state.r_start + state.block_capacity;
		GetIntersection(intersection, l_end, r_end);
		D_ASSERT(l_end <= l_count);
		D_ASSERT(r_end <= r_count);
		D_ASSERT(intersection == l_end + r_end);
	} else {
		l_end = l_count;
		r_end = r_count;
	}
	// Create slices of the data that this thread must merge
	left->SetIndices(0, 0);
	right->SetIndices(0, 0);
	left_input = left_block.CreateSlice(state.l_start, l_end, left->entry_idx);
	right_input = right_block.CreateSlice(state.r_start, r_end, right->entry_idx);
	left->sb = left_input.get();
	right->sb = right_input.get();
	state.l_start = l_end;
	state.r_start = r_end;
	D_ASSERT(left->Remaining() + right->Remaining() == state.block_capacity || (l_end == l_count && r_end == r_count));
	// Update global state
	if (state.l_start == l_count && state.r_start == r_count) {
		// Delete references to previous pair
		state.sorted_blocks[state.pair_idx * 2] = nullptr;
		state.sorted_blocks[state.pair_idx * 2 + 1] = nullptr;
		// Advance pair
		state.pair_idx++;
		state.l_start = 0;
		state.r_start = 0;
	}
}

int MergeSorter::CompareUsingGlobalIndex(SBScanState &l, SBScanState &r, const idx_t l_idx, const idx_t r_idx) {
	D_ASSERT(l_idx < l.sb->Count());
	D_ASSERT(r_idx < r.sb->Count());

	// Easy comparison using the previous result (intersections must increase monotonically)
	if (l_idx < state.l_start) {
		return -1;
	}
	if (r_idx < state.r_start) {
		return 1;
	}

	l.sb->GlobalToLocalIndex(l_idx, l.block_idx, l.entry_idx);
	r.sb->GlobalToLocalIndex(r_idx, r.block_idx, r.entry_idx);

	l.PinRadix(l.block_idx);
	r.PinRadix(r.block_idx);
	data_ptr_t l_ptr = l.radix_handle.Ptr() + l.entry_idx * sort_layout.entry_size;
	data_ptr_t r_ptr = r.radix_handle.Ptr() + r.entry_idx * sort_layout.entry_size;

	int comp_res;
	if (sort_layout.all_constant) {
		comp_res = FastMemcmp(l_ptr, r_ptr, sort_layout.comparison_size);
	} else {
		l.PinData(*l.sb->blob_sorting_data);
		r.PinData(*r.sb->blob_sorting_data);
		comp_res = Comparators::CompareTuple(l, r, l_ptr, r_ptr, sort_layout, state.external);
	}
	return comp_res;
}

void MergeSorter::GetIntersection(const idx_t diagonal, idx_t &l_idx, idx_t &r_idx) {
	const idx_t l_count = left->sb->Count();
	const idx_t r_count = right->sb->Count();
	// Cover some edge cases
	// Code coverage off because these edge cases cannot happen unless other code changes
	// Edge cases have been tested extensively while developing Merge Path in a script
	// LCOV_EXCL_START
	if (diagonal >= l_count + r_count) {
		l_idx = l_count;
		r_idx = r_count;
		return;
	} else if (diagonal == 0) {
		l_idx = 0;
		r_idx = 0;
		return;
	} else if (l_count == 0) {
		l_idx = 0;
		r_idx = diagonal;
		return;
	} else if (r_count == 0) {
		r_idx = 0;
		l_idx = diagonal;
		return;
	}
	// LCOV_EXCL_STOP
	// Determine offsets for the binary search
	const idx_t l_offset = MinValue(l_count, diagonal);
	const idx_t r_offset = diagonal > l_count ? diagonal - l_count : 0;
	D_ASSERT(l_offset + r_offset == diagonal);
	const idx_t search_space = diagonal > MaxValue(l_count, r_count) ? l_count + r_count - diagonal
	                                                                 : MinValue(diagonal, MinValue(l_count, r_count));
	// Double binary search
	idx_t li = 0;
	idx_t ri = search_space - 1;
	idx_t middle;
	int comp_res;
	while (li <= ri) {
		middle = (li + ri) / 2;
		l_idx = l_offset - middle;
		r_idx = r_offset + middle;
		if (l_idx == l_count || r_idx == 0) {
			comp_res = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
			if (comp_res > 0) {
				l_idx--;
				r_idx++;
			} else {
				return;
			}
			if (l_idx == 0 || r_idx == r_count) {
				// This case is incredibly difficult to cover as it is dependent on parallelism randomness
				// But it has been tested extensively during development in a script
				// LCOV_EXCL_START
				return;
				// LCOV_EXCL_STOP
			} else {
				break;
			}
		}
		comp_res = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx);
		if (comp_res > 0) {
			li = middle + 1;
		} else {
			ri = middle - 1;
		}
	}
	int l_r_min1 = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx - 1);
	int l_min1_r = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
	if (l_r_min1 > 0 && l_min1_r < 0) {
		return;
	} else if (l_r_min1 > 0) {
		l_idx--;
		r_idx++;
	} else if (l_min1_r < 0) {
		l_idx++;
		r_idx--;
	}
}

void MergeSorter::ComputeMerge(const idx_t &count, bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	auto &l_sorted_block = *l.sb;
	auto &r_sorted_block = *r.sb;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;
	// Data pointers for both sides
	data_ptr_t l_radix_ptr;
	data_ptr_t r_radix_ptr;
	// Compute the merge of the next 'count' tuples
	idx_t compared = 0;
	while (compared < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_sorted_block.radix_sorting_data.size() &&
		    l.entry_idx == l_sorted_block.radix_sorting_data[l.block_idx].count) {
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_sorted_block.radix_sorting_data.size() &&
		    r.entry_idx == r_sorted_block.radix_sorting_data[r.block_idx].count) {
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_sorted_block.radix_sorting_data.size();
		const bool r_done = r.block_idx == r_sorted_block.radix_sorting_data.size();
		if (l_done || r_done) {
			// One of the sides is exhausted, no need to compare
			break;
		}
		// Pin the radix sorting data
		if (!l_done) {
			left->PinRadix(l.block_idx);
			l_radix_ptr = left->RadixPtr();
		}
		if (!r_done) {
			right->PinRadix(r.block_idx);
			r_radix_ptr = right->RadixPtr();
		}
		const idx_t &l_count = !l_done ? l_sorted_block.radix_sorting_data[l.block_idx].count : 0;
		const idx_t &r_count = !r_done ? r_sorted_block.radix_sorting_data[r.block_idx].count : 0;
		// Compute the merge
		if (sort_layout.all_constant) {
			// All sorting columns are constant size
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] = FastMemcmp(l_radix_ptr, r_radix_ptr, sort_layout.comparison_size) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		} else {
			// Pin the blob data
			if (!l_done) {
				left->PinData(*l_sorted_block.blob_sorting_data);
			}
			if (!r_done) {
				right->PinData(*r_sorted_block.blob_sorting_data);
			}
			// Merge with variable size sorting columns
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] =
				    Comparators::CompareTuple(*left, *right, l_radix_ptr, r_radix_ptr, sort_layout, state.external) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeRadix(const idx_t &count, const bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	auto &l_blocks = l.sb->radix_sorting_data;
	auto &r_blocks = r.sb->radix_sorting_data;
	RowDataBlock *l_block;
	RowDataBlock *r_block;

	data_ptr_t l_ptr;
	data_ptr_t r_ptr;

	RowDataBlock *result_block = &result->radix_sorting_data.back();
	auto result_handle = buffer_manager.Pin(result_block->block);
	data_ptr_t result_ptr = result_handle.Ptr() + result_block->count * sort_layout.entry_size;

	idx_t copied = 0;
	while (copied < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_blocks.size() && l.entry_idx == l_blocks[l.block_idx].count) {
			// Delete reference to previous block
			l_blocks[l.block_idx].block = nullptr;
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_blocks.size() && r.entry_idx == r_blocks[r.block_idx].count) {
			// Delete reference to previous block
			r_blocks[r.block_idx].block = nullptr;
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_blocks.size();
		const bool r_done = r.block_idx == r_blocks.size();
		// Pin the radix sortable blocks
		if (!l_done) {
			l_block = &l_blocks[l.block_idx];
			left->PinRadix(l.block_idx);
			l_ptr = l.RadixPtr();
		}
		if (!r_done) {
			r_block = &r_blocks[r.block_idx];
			r.PinRadix(r.block_idx);
			r_ptr = r.RadixPtr();
		}
		const idx_t &l_count = !l_done ? l_block->count : 0;
		const idx_t &r_count = !r_done ? r_block->count : 0;
		// Copy using computed merge
		if (!l_done && !r_done) {
			// Both sides have data - merge
			MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, result_block, result_ptr,
			          sort_layout.entry_size, left_smaller, copied, count);
		} else if (r_done) {
			// Right side is exhausted
			FlushRows(l_ptr, l.entry_idx, l_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		} else {
			// Left side is exhausted
			FlushRows(r_ptr, r.entry_idx, r_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
                            const bool left_smaller[], idx_t next_entry_sizes[], bool reset_indices) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	const auto &layout = result_data.layout;
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();

	// Left and right row data to merge
	data_ptr_t l_ptr;
	data_ptr_t r_ptr;
	// Accompanying left and right heap data (if needed)
	data_ptr_t l_heap_ptr;
	data_ptr_t r_heap_ptr;

	// Result rows to write to
	RowDataBlock *result_data_block = &result_data.data_blocks.back();
	auto result_data_handle = buffer_manager.Pin(result_data_block->block);
	data_ptr_t result_data_ptr = result_data_handle.Ptr() + result_data_block->count * row_width;
	// Result heap to write to (if needed)
	RowDataBlock *result_heap_block;
	BufferHandle result_heap_handle;
	data_ptr_t result_heap_ptr;
	if (!layout.AllConstant() && state.external) {
		result_heap_block = &result_data.heap_blocks.back();
		result_heap_handle = buffer_manager.Pin(result_heap_block->block);
		result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
	}

	idx_t copied = 0;
	while (copied < count) {
		// Move to new data blocks (if needed)
		if (l.block_idx < l_data.data_blocks.size() && l.entry_idx == l_data.data_blocks[l.block_idx].count) {
			// Delete reference to previous block
			l_data.data_blocks[l.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				l_data.heap_blocks[l.block_idx].block = nullptr;
			}
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_data.data_blocks.size() && r.entry_idx == r_data.data_blocks[r.block_idx].count) {
			// Delete reference to previous block
			r_data.data_blocks[r.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				r_data.heap_blocks[r.block_idx].block = nullptr;
			}
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_data.data_blocks.size();
		const bool r_done = r.block_idx == r_data.data_blocks.size();
		// Pin the row data blocks
		if (!l_done) {
			l.PinData(l_data);
			l_ptr = l.DataPtr(l_data);
		}
		if (!r_done) {
			r.PinData(r_data);
			r_ptr = r.DataPtr(r_data);
		}
		const idx_t &l_count = !l_done ? l_data.data_blocks[l.block_idx].count : 0;
		const idx_t &r_count = !r_done ? r_data.data_blocks[r.block_idx].count : 0;
		// Perform the merge
		if (layout.AllConstant() || !state.external) {
			// If all constant size, or if we are doing an in-memory sort, we do not need to touch the heap
			if (!l_done && !r_done) {
				// Both sides have data - merge
				MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, result_data_block, result_data_ptr,
				          row_width, left_smaller, copied, count);
			} else if (r_done) {
				// Right side is exhausted
				FlushRows(l_ptr, l.entry_idx, l_count, result_data_block, result_data_ptr, row_width, copied, count);
			} else {
				// Left side is exhausted
				FlushRows(r_ptr, r.entry_idx, r_count, result_data_block, result_data_ptr, row_width, copied, count);
			}
		} else {
			// External sorting with variable size data. Pin the heap blocks too
			if (!l_done) {
				l_heap_ptr = l.BaseHeapPtr(l_data) + Load<idx_t>(l_ptr + heap_pointer_offset);
				D_ASSERT(l_heap_ptr - l.BaseHeapPtr(l_data) >= 0);
				D_ASSERT((idx_t)(l_heap_ptr - l.BaseHeapPtr(l_data)) < l_data.heap_blocks[l.block_idx].byte_offset);
			}
			if (!r_done) {
				r_heap_ptr = r.BaseHeapPtr(r_data) + Load<idx_t>(r_ptr + heap_pointer_offset);
				D_ASSERT(r_heap_ptr - r.BaseHeapPtr(r_data) >= 0);
				D_ASSERT((idx_t)(r_heap_ptr - r.BaseHeapPtr(r_data)) < r_data.heap_blocks[r.block_idx].byte_offset);
			}
			// Both the row and heap data need to be dealt with
			if (!l_done && !r_done) {
				// Both sides have data - merge
				idx_t l_idx_copy = l.entry_idx;
				idx_t r_idx_copy = r.entry_idx;
				data_ptr_t result_data_ptr_copy = result_data_ptr;
				idx_t copied_copy = copied;
				// Merge row data
				MergeRows(l_ptr, l_idx_copy, l_count, r_ptr, r_idx_copy, r_count, result_data_block,
				          result_data_ptr_copy, row_width, left_smaller, copied_copy, count);
				const idx_t merged = copied_copy - copied;
				// Compute the entry sizes and number of heap bytes that will be copied
				idx_t copy_bytes = 0;
				data_ptr_t l_heap_ptr_copy = l_heap_ptr;
				data_ptr_t r_heap_ptr_copy = r_heap_ptr;
				for (idx_t i = 0; i < merged; i++) {
					// Store base heap offset in the row data
					Store<idx_t>(result_heap_block->byte_offset + copy_bytes, result_data_ptr + heap_pointer_offset);
					result_data_ptr += row_width;
					// Compute entry size and add to total
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					auto &entry_size = next_entry_sizes[copied + i];
					entry_size =
					    l_smaller * Load<uint32_t>(l_heap_ptr_copy) + r_smaller * Load<uint32_t>(r_heap_ptr_copy);
					D_ASSERT(entry_size >= sizeof(uint32_t));
					D_ASSERT(l_heap_ptr_copy - l.BaseHeapPtr(l_data) + l_smaller * entry_size <=
					         l_data.heap_blocks[l.block_idx].byte_offset);
					D_ASSERT(r_heap_ptr_copy - r.BaseHeapPtr(r_data) + r_smaller * entry_size <=
					         r_data.heap_blocks[r.block_idx].byte_offset);
					l_heap_ptr_copy += l_smaller * entry_size;
					r_heap_ptr_copy += r_smaller * entry_size;
					copy_bytes += entry_size;
				}
				// Reallocate result heap block size (if needed)
				if (result_heap_block->byte_offset + copy_bytes > result_heap_block->capacity) {
					idx_t new_capacity = result_heap_block->byte_offset + copy_bytes;
					buffer_manager.ReAllocate(result_heap_block->block, new_capacity);
					result_heap_block->capacity = new_capacity;
					result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
				}
				D_ASSERT(result_heap_block->byte_offset + copy_bytes <= result_heap_block->capacity);
				// Now copy the heap data
				for (idx_t i = 0; i < merged; i++) {
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					const auto &entry_size = next_entry_sizes[copied + i];
					memcpy(result_heap_ptr, (data_ptr_t)(l_smaller * (idx_t)l_heap_ptr + r_smaller * (idx_t)r_heap_ptr),
					       entry_size);
					D_ASSERT(Load<uint32_t>(result_heap_ptr) == entry_size);
					result_heap_ptr += entry_size;
					l_heap_ptr += l_smaller * entry_size;
					r_heap_ptr += r_smaller * entry_size;
					l.entry_idx += l_smaller;
					r.entry_idx += r_smaller;
				}
				// Update result indices and pointers
				result_heap_block->count += merged;
				result_heap_block->byte_offset += copy_bytes;
				copied += merged;
			} else if (r_done) {
				// Right side is exhausted - flush left
				FlushBlobs(layout, l_count, l_ptr, l.entry_idx, l_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			} else {
				// Left side is exhausted - flush right
				FlushBlobs(layout, r_count, r_ptr, r.entry_idx, r_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			}
			D_ASSERT(result_data_block->count == result_heap_block->count);
		}
	}
	if (reset_indices) {
		left->SetIndices(l_block_idx_before, l_entry_idx_before);
		right->SetIndices(r_block_idx_before, r_entry_idx_before);
	}
}

void MergeSorter::MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr,
                            idx_t &r_entry_idx, const idx_t &r_count, RowDataBlock *target_block,
                            data_ptr_t &target_ptr, const idx_t &entry_size, const bool left_smaller[], idx_t &copied,
                            const idx_t &count) {
	const idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
	idx_t i;
	for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
		const bool &l_smaller = left_smaller[copied + i];
		const bool r_smaller = !l_smaller;
		// Use comparison bool (0 or 1) to copy an entry from either side
		FastMemcpy(target_ptr, (data_ptr_t)(l_smaller * (idx_t)l_ptr + r_smaller * (idx_t)r_ptr), entry_size);
		target_ptr += entry_size;
		// Use the comparison bool to increment entries and pointers
		l_entry_idx += l_smaller;
		r_entry_idx += r_smaller;
		l_ptr += l_smaller * entry_size;
		r_ptr += r_smaller * entry_size;
	}
	// Update counts
	target_block->count += i;
	copied += i;
}

void MergeSorter::FlushRows(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
                            RowDataBlock *target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
                            const idx_t &count) {
	// Compute how many entries we can fit
	idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
	next = MinValue(next, source_count - source_entry_idx);
	// Copy them all in a single memcpy
	const idx_t copy_bytes = next * entry_size;
	memcpy(target_ptr, source_ptr, copy_bytes);
	target_ptr += copy_bytes;
	source_ptr += copy_bytes;
	// Update counts
	source_entry_idx += next;
	target_block->count += next;
	copied += next;
}

void MergeSorter::FlushBlobs(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
                             idx_t &source_entry_idx, data_ptr_t &source_heap_ptr, RowDataBlock *target_data_block,
                             data_ptr_t &target_data_ptr, RowDataBlock *target_heap_block,
                             BufferHandle &target_heap_handle, data_ptr_t &target_heap_ptr, idx_t &copied,
                             const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	idx_t source_entry_idx_copy = source_entry_idx;
	data_ptr_t target_data_ptr_copy = target_data_ptr;
	idx_t copied_copy = copied;
	// Flush row data
	FlushRows(source_data_ptr, source_entry_idx_copy, source_count, target_data_block, target_data_ptr_copy, row_width,
	          copied_copy, count);
	const idx_t flushed = copied_copy - copied;
	// Compute the entry sizes and number of heap bytes that will be copied
	idx_t copy_bytes = 0;
	data_ptr_t source_heap_ptr_copy = source_heap_ptr;
	for (idx_t i = 0; i < flushed; i++) {
		// Store base heap offset in the row data
		Store<idx_t>(target_heap_block->byte_offset + copy_bytes, target_data_ptr + heap_pointer_offset);
		target_data_ptr += row_width;
		// Compute entry size and add to total
		auto entry_size = Load<uint32_t>(source_heap_ptr_copy);
		D_ASSERT(entry_size >= sizeof(uint32_t));
		source_heap_ptr_copy += entry_size;
		copy_bytes += entry_size;
	}
	// Reallocate result heap block size (if needed)
	if (target_heap_block->byte_offset + copy_bytes > target_heap_block->capacity) {
		idx_t new_capacity = target_heap_block->byte_offset + copy_bytes;
		buffer_manager.ReAllocate(target_heap_block->block, new_capacity);
		target_heap_block->capacity = new_capacity;
		target_heap_ptr = target_heap_handle.Ptr() + target_heap_block->byte_offset;
	}
	D_ASSERT(target_heap_block->byte_offset + copy_bytes <= target_heap_block->capacity);
	// Copy the heap data in one go
	memcpy(target_heap_ptr, source_heap_ptr, copy_bytes);
	target_heap_ptr += copy_bytes;
	source_heap_ptr += copy_bytes;
	source_entry_idx += flushed;
	copied += flushed;
	// Update result indices and pointers
	target_heap_block->count += flushed;
	target_heap_block->byte_offset += copy_bytes;
	D_ASSERT(target_heap_block->byte_offset <= target_heap_block->capacity);
}

} // namespace duckdb




namespace duckdb {

//! Calls std::sort on strings that are tied by their prefix after the radix sort
static void SortTiedBlobs(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start, const idx_t &end,
                          const idx_t &tie_col, bool *ties, const data_ptr_t blob_ptr, const SortLayout &sort_layout) {
	const auto row_width = sort_layout.blob_layout.GetRowWidth();
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	// Locate the first blob row in question
	data_ptr_t row_ptr = dataptr + start * sort_layout.entry_size;
	data_ptr_t blob_row_ptr = blob_ptr + Load<uint32_t>(row_ptr + sort_layout.comparison_size) * row_width;
	if (!Comparators::TieIsBreakable(col_idx, blob_row_ptr, sort_layout.blob_layout)) {
		// Quick check to see if ties can be broken
		return;
	}
	// Fill pointer array for sorting
	auto ptr_block = unique_ptr<data_ptr_t[]>(new data_ptr_t[end - start]);
	auto entry_ptrs = (data_ptr_t *)ptr_block.get();
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = row_ptr;
		row_ptr += sort_layout.entry_size;
	}
	// Slow pointer-based sorting
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	auto logical_type = sort_layout.blob_layout.GetTypes()[col_idx];
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&blob_ptr, &order, &sort_layout, &tie_col_offset, &row_width, &logical_type](const data_ptr_t l,
	                                                                                        const data_ptr_t r) {
		          idx_t left_idx = Load<uint32_t>(l + sort_layout.comparison_size);
		          idx_t right_idx = Load<uint32_t>(r + sort_layout.comparison_size);
		          data_ptr_t left_ptr = blob_ptr + left_idx * row_width + tie_col_offset;
		          data_ptr_t right_ptr = blob_ptr + right_idx * row_width + tie_col_offset;
		          return order * Comparators::CompareVal(left_ptr, right_ptr, logical_type) < 0;
	          });
	// Re-order
	auto temp_block =
	    buffer_manager.Allocate(MaxValue((end - start) * sort_layout.entry_size, (idx_t)Storage::BLOCK_SIZE));
	data_ptr_t temp_ptr = temp_block.Ptr();
	for (idx_t i = 0; i < end - start; i++) {
		FastMemcpy(temp_ptr, entry_ptrs[i], sort_layout.entry_size);
		temp_ptr += sort_layout.entry_size;
	}
	memcpy(dataptr + start * sort_layout.entry_size, temp_block.Ptr(), (end - start) * sort_layout.entry_size);
	// Determine if there are still ties (if this is not the last column)
	if (tie_col < sort_layout.column_count - 1) {
		data_ptr_t idx_ptr = dataptr + start * sort_layout.entry_size + sort_layout.comparison_size;
		// Load current entry
		data_ptr_t current_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
		for (idx_t i = 0; i < end - start - 1; i++) {
			// Load next entry and compare
			idx_ptr += sort_layout.entry_size;
			data_ptr_t next_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
			ties[start + i] = Comparators::CompareVal(current_ptr, next_ptr, logical_type) == 0;
			current_ptr = next_ptr;
		}
	}
}

//! Identifies sequences of rows that are tied by the prefix of a blob column, and sorts them
static void SortTiedBlobs(BufferManager &buffer_manager, SortedBlock &sb, bool *ties, data_ptr_t dataptr,
                          const idx_t &count, const idx_t &tie_col, const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	auto &blob_block = sb.blob_sorting_data->data_blocks.back();
	auto blob_handle = buffer_manager.Pin(blob_block.block);
	const data_ptr_t blob_ptr = blob_handle.Ptr();

	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		SortTiedBlobs(buffer_manager, dataptr, i, j + 1, tie_col, ties, blob_ptr, sort_layout);
		i = j;
	}
}

//! Returns whether there are any 'true' values in the ties[] array
static bool AnyTies(bool ties[], const idx_t &count) {
	D_ASSERT(!ties[count - 1]);
	bool any_ties = false;
	for (idx_t i = 0; i < count - 1; i++) {
		any_ties = any_ties || ties[i];
	}
	return any_ties;
}

//! Compares subsequent rows to check for ties
static void ComputeTies(data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset, const idx_t &tie_size,
                        bool ties[], const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	D_ASSERT(col_offset + tie_size <= sort_layout.comparison_size);
	// Align dataptr
	dataptr += col_offset;
	for (idx_t i = 0; i < count - 1; i++) {
		ties[i] = ties[i] && FastMemcmp(dataptr, dataptr + sort_layout.entry_size, tie_size) == 0;
		dataptr += sort_layout.entry_size;
	}
}

//! Textbook LSD radix sort
void RadixSortLSD(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &sorting_size) {
	auto temp_block = buffer_manager.Allocate(MaxValue(count * row_width, (idx_t)Storage::BLOCK_SIZE));
	bool swap = false;

	idx_t counts[SortConstants::VALUES_PER_RADIX];
	for (idx_t r = 1; r <= sorting_size; r++) {
		// Init counts to 0
		memset(counts, 0, sizeof(counts));
		// Const some values for convenience
		const data_ptr_t source_ptr = swap ? temp_block.Ptr() : dataptr;
		const data_ptr_t target_ptr = swap ? dataptr : temp_block.Ptr();
		const idx_t offset = col_offset + sorting_size - r;
		// Collect counts
		data_ptr_t offset_ptr = source_ptr + offset;
		for (idx_t i = 0; i < count; i++) {
			counts[*offset_ptr]++;
			offset_ptr += row_width;
		}
		// Compute offsets from counts
		idx_t max_count = counts[0];
		for (idx_t val = 1; val < SortConstants::VALUES_PER_RADIX; val++) {
			max_count = MaxValue<idx_t>(max_count, counts[val]);
			counts[val] = counts[val] + counts[val - 1];
		}
		if (max_count == count) {
			continue;
		}
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr + (count - 1) * row_width;
		for (idx_t i = 0; i < count; i++) {
			idx_t &radix_offset = --counts[*(row_ptr + offset)];
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr -= row_width;
		}
		swap = !swap;
	}
	// Move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(dataptr, temp_block.Ptr(), count * row_width);
	}
}

//! Insertion sort, used when count of values is low
inline void InsertionSort(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count,
                          const idx_t &col_offset, const idx_t &row_width, const idx_t &total_comp_width,
                          const idx_t &offset, bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	if (count > 1) {
		const idx_t total_offset = col_offset + offset;
		auto temp_val = unique_ptr<data_t[]>(new data_t[row_width]);
		const data_ptr_t val = temp_val.get();
		const auto comp_width = total_comp_width - offset;
		for (idx_t i = 1; i < count; i++) {
			FastMemcpy(val, source_ptr + i * row_width, row_width);
			idx_t j = i;
			while (j > 0 &&
			       FastMemcmp(source_ptr + (j - 1) * row_width + total_offset, val + total_offset, comp_width) > 0) {
				FastMemcpy(source_ptr + j * row_width, source_ptr + (j - 1) * row_width, row_width);
				j--;
			}
			FastMemcpy(source_ptr + j * row_width, val, row_width);
		}
	}
	if (swap) {
		memcpy(target_ptr, source_ptr, count * row_width);
	}
}

//! MSD radix sort that switches to insertion sort with low bucket sizes
void RadixSortMSD(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &comp_width, const idx_t &offset, idx_t locations[], bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	// Init counts to 0
	memset(locations, 0, SortConstants::MSD_RADIX_LOCATIONS * sizeof(idx_t));
	idx_t *counts = locations + 1;
	// Collect counts
	const idx_t total_offset = col_offset + offset;
	data_ptr_t offset_ptr = source_ptr + total_offset;
	for (idx_t i = 0; i < count; i++) {
		counts[*offset_ptr]++;
		offset_ptr += row_width;
	}
	// Compute locations from counts
	idx_t max_count = 0;
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		max_count = MaxValue<idx_t>(max_count, counts[radix]);
		counts[radix] += locations[radix];
	}
	if (max_count != count) {
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr;
		for (idx_t i = 0; i < count; i++) {
			const idx_t &radix_offset = locations[*(row_ptr + total_offset)]++;
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr += row_width;
		}
		swap = !swap;
	}
	// Check if done
	if (offset == comp_width - 1) {
		if (swap) {
			memcpy(orig_ptr, temp_ptr, count * row_width);
		}
		return;
	}
	if (max_count == count) {
		RadixSortMSD(orig_ptr, temp_ptr, count, col_offset, row_width, comp_width, offset + 1,
		             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		return;
	}
	// Recurse
	idx_t radix_count = locations[0];
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		const idx_t loc = (locations[radix] - radix_count) * row_width;
		if (radix_count > SortConstants::INSERTION_SORT_THRESHOLD) {
			RadixSortMSD(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		} else if (radix_count != 0) {
			InsertionSort(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			              swap);
		}
		radix_count = locations[radix + 1] - locations[radix];
	}
}

//! Calls different sort functions, depending on the count and sorting sizes
void RadixSort(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
               const idx_t &sorting_size, const SortLayout &sort_layout) {
	if (count <= SortConstants::INSERTION_SORT_THRESHOLD) {
		InsertionSort(dataptr, nullptr, count, 0, sort_layout.entry_size, sort_layout.comparison_size, 0, false);
	} else if (sorting_size <= SortConstants::MSD_RADIX_SORT_SIZE_THRESHOLD) {
		RadixSortLSD(buffer_manager, dataptr, count, col_offset, sort_layout.entry_size, sorting_size);
	} else {
		auto temp_block = buffer_manager.Allocate(MaxValue(count * sort_layout.entry_size, (idx_t)Storage::BLOCK_SIZE));
		auto preallocated_array = unique_ptr<idx_t[]>(new idx_t[sorting_size * SortConstants::MSD_RADIX_LOCATIONS]);
		RadixSortMSD(dataptr, temp_block.Ptr(), count, col_offset, sort_layout.entry_size, sorting_size, 0,
		             preallocated_array.get(), false);
	}
}

//! Identifies sequences of rows that are tied, and calls radix sort on these
static void SubSortTiedTuples(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &count,
                              const idx_t &col_offset, const idx_t &sorting_size, bool ties[],
                              const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i + 1; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		RadixSort(buffer_manager, dataptr + i * sort_layout.entry_size, j - i + 1, col_offset, sorting_size,
		          sort_layout);
		i = j;
	}
}

void LocalSortState::SortInMemory() {
	auto &sb = *sorted_blocks.back();
	auto &block = sb.radix_sorting_data.back();
	const auto &count = block.count;
	auto handle = buffer_manager->Pin(block.block);
	const auto dataptr = handle.Ptr();
	// Assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sort_layout->comparison_size;
	for (uint32_t i = 0; i < count; i++) {
		Store<uint32_t>(i, idx_dataptr);
		idx_dataptr += sort_layout->entry_size;
	}
	// Radix sort and break ties until no more ties, or until all columns are sorted
	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unique_ptr<bool[]> ties_ptr;
	bool *ties = nullptr;
	for (idx_t i = 0; i < sort_layout->column_count; i++) {
		sorting_size += sort_layout->column_sizes[i];
		if (sort_layout->constant_size[i] && i < sort_layout->column_count - 1) {
			// Add columns to the sorting size until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// This is the first sort
			RadixSort(*buffer_manager, dataptr, count, col_offset, sorting_size, *sort_layout);
			ties_ptr = unique_ptr<bool[]>(new bool[count]);
			ties = ties_ptr.get();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// For subsequent sorts, we only have to subsort the tied tuples
			SubSortTiedTuples(*buffer_manager, dataptr, count, col_offset, sorting_size, ties, *sort_layout);
		}

		if (sort_layout->constant_size[i] && i == sort_layout->column_count - 1) {
			// All columns are sorted, no ties to break because last column is constant size
			break;
		}

		ComputeTies(dataptr, count, col_offset, sorting_size, ties, *sort_layout);
		if (!AnyTies(ties, count)) {
			// No ties, stop sorting
			break;
		}

		if (!sort_layout->constant_size[i]) {
			SortTiedBlobs(*buffer_manager, sb, ties, dataptr, count, i, *sort_layout);
			if (!AnyTies(ties, count)) {
				// No more ties after tie-breaking, stop
				break;
			}
		}

		col_offset += sorting_size;
		sorting_size = 0;
	}
}

} // namespace duckdb






#include <numeric>

namespace duckdb {

idx_t GetNestedSortingColSize(idx_t &col_size, const LogicalType &type) {
	auto physical_type = type.InternalType();
	if (TypeIsConstantSize(physical_type)) {
		col_size += GetTypeIdSize(physical_type);
		return 0;
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR: {
			// Nested strings are between 4 and 11 chars long for alignment
			auto size_before_str = col_size;
			col_size += 11;
			col_size -= (col_size - 12) % 8;
			return col_size - size_before_str;
		}
		case PhysicalType::LIST:
			// Lists get 2 bytes (null and empty list)
			col_size += 2;
			return GetNestedSortingColSize(col_size, ListType::GetChildType(type));
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			// Structs get 1 bytes (null)
			col_size++;
			return GetNestedSortingColSize(col_size, StructType::GetChildType(type, 0));
		default:
			throw NotImplementedException("Unable to order column with type %s", type.ToString());
		}
	}
}

SortLayout::SortLayout(const vector<BoundOrderByNode> &orders)
    : column_count(orders.size()), all_constant(true), comparison_size(0), entry_size(0) {
	vector<LogicalType> blob_layout_types;
	for (idx_t i = 0; i < column_count; i++) {
		const auto &order = orders[i];

		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		auto &expr = *order.expression;
		logical_types.push_back(expr.return_type);

		auto physical_type = expr.return_type.InternalType();
		constant_size.push_back(TypeIsConstantSize(physical_type));

		if (order.stats) {
			stats.push_back(order.stats.get());
			has_null.push_back(stats.back()->CanHaveNull());
		} else {
			stats.push_back(nullptr);
			has_null.push_back(true);
		}

		idx_t col_size = has_null.back() ? 1 : 0;
		prefix_lengths.push_back(0);
		if (!TypeIsConstantSize(physical_type) && physical_type != PhysicalType::VARCHAR) {
			prefix_lengths.back() = GetNestedSortingColSize(col_size, expr.return_type);
		} else if (physical_type == PhysicalType::VARCHAR) {
			idx_t size_before = col_size;
			if (stats.back()) {
				auto &str_stats = (StringStatistics &)*stats.back();
				col_size += str_stats.max_string_length;
				if (col_size > 12) {
					col_size = 12;
				} else {
					constant_size.back() = true;
				}
			} else {
				col_size = 12;
			}
			prefix_lengths.back() = col_size - size_before;
		} else {
			col_size += GetTypeIdSize(physical_type);
		}

		comparison_size += col_size;
		column_sizes.push_back(col_size);
	}
	entry_size = comparison_size + sizeof(uint32_t);

	// 8-byte alignment
	if (entry_size % 8 != 0) {
		// First assign more bytes to strings instead of aligning
		idx_t bytes_to_fill = 8 - (entry_size % 8);
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			if (bytes_to_fill == 0) {
				break;
			}
			if (logical_types[col_idx].InternalType() == PhysicalType::VARCHAR && stats[col_idx]) {
				auto &str_stats = (StringStatistics &)*stats[col_idx];
				idx_t diff = str_stats.max_string_length - prefix_lengths[col_idx];
				if (diff > 0) {
					// Increase all sizes accordingly
					idx_t increase = MinValue(bytes_to_fill, diff);
					column_sizes[col_idx] += increase;
					prefix_lengths[col_idx] += increase;
					constant_size[col_idx] = increase == diff;
					comparison_size += increase;
					entry_size += increase;
					bytes_to_fill -= increase;
				}
			}
		}
		entry_size = AlignValue(entry_size);
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		all_constant = all_constant && constant_size[col_idx];
		if (!constant_size[col_idx]) {
			sorting_to_blob_col[col_idx] = blob_layout_types.size();
			blob_layout_types.push_back(logical_types[col_idx]);
		}
	}

	blob_layout.Initialize(blob_layout_types);
}

LocalSortState::LocalSortState() : initialized(false) {
}

static idx_t EntriesPerBlock(idx_t width) {
	return (Storage::BLOCK_SIZE + width * STANDARD_VECTOR_SIZE - 1) / width;
}

void LocalSortState::Initialize(GlobalSortState &global_sort_state, BufferManager &buffer_manager_p) {
	sort_layout = &global_sort_state.sort_layout;
	payload_layout = &global_sort_state.payload_layout;
	buffer_manager = &buffer_manager_p;
	// Radix sorting data
	radix_sorting_data = make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(sort_layout->entry_size),
	                                                    sort_layout->entry_size);
	// Blob sorting data
	if (!sort_layout->all_constant) {
		auto blob_row_width = sort_layout->blob_layout.GetRowWidth();
		blob_sorting_data =
		    make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(blob_row_width), blob_row_width);
		blob_sorting_heap = make_unique<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	// Payload data
	auto payload_row_width = payload_layout->GetRowWidth();
	payload_data =
	    make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(payload_row_width), payload_row_width);
	payload_heap = make_unique<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	// Init done
	initialized = true;
}

void LocalSortState::SinkChunk(DataChunk &sort, DataChunk &payload) {
	D_ASSERT(sort.size() == payload.size());
	// Build and serialize sorting data to radix sortable rows
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto handles = radix_sorting_data->Build(sort.size(), data_pointers, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sort_layout->has_null[sort_col];
		bool nulls_first = sort_layout->order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sort_layout->order_types[sort_col] == OrderType::DESCENDING;
		RowOperations::RadixScatter(sort.data[sort_col], sort.size(), sel_ptr, sort.size(), data_pointers, desc,
		                            has_null, nulls_first, sort_layout->prefix_lengths[sort_col],
		                            sort_layout->column_sizes[sort_col]);
	}

	// Also fully serialize blob sorting columns (to be able to break ties
	if (!sort_layout->all_constant) {
		DataChunk blob_chunk;
		blob_chunk.SetCardinality(sort.size());
		for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
			if (!sort_layout->constant_size[sort_col]) {
				blob_chunk.data.emplace_back(sort.data[sort_col]);
			}
		}
		handles = blob_sorting_data->Build(blob_chunk.size(), data_pointers, nullptr);
		auto blob_data = blob_chunk.Orrify();
		RowOperations::Scatter(blob_chunk, blob_data.get(), sort_layout->blob_layout, addresses, *blob_sorting_heap,
		                       sel_ptr, blob_chunk.size());
	}

	// Finally, serialize payload data
	handles = payload_data->Build(payload.size(), data_pointers, nullptr);
	auto input_data = payload.Orrify();
	RowOperations::Scatter(payload, input_data.get(), *payload_layout, addresses, *payload_heap, sel_ptr,
	                       payload.size());
}

idx_t LocalSortState::SizeInBytes() const {
	idx_t size_in_bytes = radix_sorting_data->SizeInBytes() + payload_data->SizeInBytes();
	if (!sort_layout->all_constant) {
		size_in_bytes += blob_sorting_data->SizeInBytes() + blob_sorting_heap->SizeInBytes();
	}
	if (!payload_layout->AllConstant()) {
		size_in_bytes += payload_heap->SizeInBytes();
	}
	return size_in_bytes;
}

void LocalSortState::Sort(GlobalSortState &global_sort_state, bool reorder_heap) {
	D_ASSERT(radix_sorting_data->count == payload_data->count);
	if (radix_sorting_data->count == 0) {
		return;
	}
	// Move all data to a single SortedBlock
	sorted_blocks.emplace_back(make_unique<SortedBlock>(*buffer_manager, global_sort_state));
	auto &sb = *sorted_blocks.back();
	// Fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(*radix_sorting_data);
	sb.radix_sorting_data.push_back(move(sorting_block));
	// Variable-size sorting data
	if (!sort_layout->all_constant) {
		auto &blob_data = *blob_sorting_data;
		auto new_block = ConcatenateBlocks(blob_data);
		sb.blob_sorting_data->data_blocks.push_back(move(new_block));
	}
	// Payload data
	auto payload_block = ConcatenateBlocks(*payload_data);
	sb.payload_data->data_blocks.push_back(move(payload_block));
	// Now perform the actual sort
	SortInMemory();
	// Re-order before the merge sort
	ReOrder(global_sort_state, reorder_heap);
}

RowDataBlock LocalSortState::ConcatenateBlocks(RowDataCollection &row_data) {
	// Create block with the correct capacity
	const idx_t &entry_size = row_data.entry_size;
	idx_t capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + entry_size - 1) / entry_size, row_data.count);
	RowDataBlock new_block(*buffer_manager, capacity, entry_size);
	new_block.count = row_data.count;
	auto new_block_handle = buffer_manager->Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle.Ptr();
	// Copy the data of the blocks into a single block
	for (auto &block : row_data.blocks) {
		auto block_handle = buffer_manager->Pin(block.block);
		memcpy(new_block_ptr, block_handle.Ptr(), block.count * entry_size);
		new_block_ptr += block.count * entry_size;
	}
	row_data.blocks.clear();
	row_data.count = 0;
	return new_block;
}

void LocalSortState::ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate,
                             bool reorder_heap) {
	sd.swizzled = reorder_heap;
	auto &unordered_data_block = sd.data_blocks.back();
	const idx_t &count = unordered_data_block.count;
	auto unordered_data_handle = buffer_manager->Pin(unordered_data_block.block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle.Ptr();
	// Create new block that will hold re-ordered row data
	RowDataBlock ordered_data_block(*buffer_manager, unordered_data_block.capacity, unordered_data_block.entry_size);
	ordered_data_block.count = count;
	auto ordered_data_handle = buffer_manager->Pin(ordered_data_block.block);
	data_ptr_t ordered_data_ptr = ordered_data_handle.Ptr();
	// Re-order fixed-size row layout
	const idx_t row_width = sd.layout.GetRowWidth();
	const idx_t sorting_entry_size = gstate.sort_layout.entry_size;
	for (idx_t i = 0; i < count; i++) {
		auto index = Load<uint32_t>(sorting_ptr);
		FastMemcpy(ordered_data_ptr, unordered_data_ptr + index * row_width, row_width);
		ordered_data_ptr += row_width;
		sorting_ptr += sorting_entry_size;
	}
	// Replace the unordered data block with the re-ordered data block
	sd.data_blocks.clear();
	sd.data_blocks.push_back(move(ordered_data_block));
	// Deal with the heap (if necessary)
	if (!sd.layout.AllConstant() && reorder_heap) {
		// Swizzle the column pointers to offsets
		RowOperations::SwizzleColumns(sd.layout, ordered_data_handle.Ptr(), count);
		// Create a single heap block to store the ordered heap
		idx_t total_byte_offset = std::accumulate(heap.blocks.begin(), heap.blocks.end(), 0,
		                                          [](idx_t a, const RowDataBlock &b) { return a + b.byte_offset; });
		idx_t heap_block_size = MaxValue(total_byte_offset, (idx_t)Storage::BLOCK_SIZE);
		RowDataBlock ordered_heap_block(*buffer_manager, heap_block_size, 1);
		ordered_heap_block.count = count;
		ordered_heap_block.byte_offset = total_byte_offset;
		auto ordered_heap_handle = buffer_manager->Pin(ordered_heap_block.block);
		data_ptr_t ordered_heap_ptr = ordered_heap_handle.Ptr();
		// Fill the heap in order
		ordered_data_ptr = ordered_data_handle.Ptr();
		const idx_t heap_pointer_offset = sd.layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < count; i++) {
			auto heap_row_ptr = Load<data_ptr_t>(ordered_data_ptr + heap_pointer_offset);
			auto heap_row_size = Load<uint32_t>(heap_row_ptr);
			memcpy(ordered_heap_ptr, heap_row_ptr, heap_row_size);
			ordered_heap_ptr += heap_row_size;
			ordered_data_ptr += row_width;
		}
		// Swizzle the base pointer to the offset of each row in the heap
		RowOperations::SwizzleHeapPointer(sd.layout, ordered_data_handle.Ptr(), ordered_heap_handle.Ptr(), count);
		// Move the re-ordered heap to the SortedData, and clear the local heap
		sd.heap_blocks.push_back(move(ordered_heap_block));
		heap.pinned_blocks.clear();
		heap.blocks.clear();
		heap.count = 0;
	}
}

void LocalSortState::ReOrder(GlobalSortState &gstate, bool reorder_heap) {
	auto &sb = *sorted_blocks.back();
	auto sorting_handle = buffer_manager->Pin(sb.radix_sorting_data.back().block);
	const data_ptr_t sorting_ptr = sorting_handle.Ptr() + gstate.sort_layout.comparison_size;
	// Re-order variable size sorting columns
	if (!gstate.sort_layout.all_constant) {
		ReOrder(*sb.blob_sorting_data, sorting_ptr, *blob_sorting_heap, gstate, reorder_heap);
	}
	// And the payload
	ReOrder(*sb.payload_data, sorting_ptr, *payload_heap, gstate, reorder_heap);
}

GlobalSortState::GlobalSortState(BufferManager &buffer_manager, const vector<BoundOrderByNode> &orders,
                                 RowLayout &payload_layout)
    : buffer_manager(buffer_manager), sort_layout(SortLayout(orders)), payload_layout(payload_layout),
      block_capacity(0), external(false) {
}

void GlobalSortState::AddLocalState(LocalSortState &local_sort_state) {
	if (!local_sort_state.radix_sorting_data) {
		return;
	}

	// Sort accumulated data
	// we only re-order the heap when the data is expected to not fit in memory
	// re-ordering the heap avoids random access when reading/merging but incurs a significant cost of shuffling data
	// when data fits in memory, doing random access on reads is cheaper than re-shuffling
	local_sort_state.Sort(*this, external || !local_sort_state.sorted_blocks.empty());

	// Append local state sorted data to this global state
	lock_guard<mutex> append_guard(lock);
	for (auto &sb : local_sort_state.sorted_blocks) {
		sorted_blocks.push_back(move(sb));
	}
	auto &payload_heap = local_sort_state.payload_heap;
	for (idx_t i = 0; i < payload_heap->blocks.size(); i++) {
		heap_blocks.push_back(move(payload_heap->blocks[i]));
		pinned_blocks.push_back(move(payload_heap->pinned_blocks[i]));
	}
	if (!sort_layout.all_constant) {
		auto &blob_heap = local_sort_state.blob_sorting_heap;
		for (idx_t i = 0; i < blob_heap->blocks.size(); i++) {
			heap_blocks.push_back(move(blob_heap->blocks[i]));
			pinned_blocks.push_back(move(blob_heap->pinned_blocks[i]));
		}
	}
}

void GlobalSortState::PrepareMergePhase() {
	// Determine if we need to use do an external sort
	idx_t total_heap_size =
	    std::accumulate(sorted_blocks.begin(), sorted_blocks.end(), (idx_t)0,
	                    [](idx_t a, const unique_ptr<SortedBlock> &b) { return a + b->HeapSize(); });
	if (external || (pinned_blocks.empty() && total_heap_size > 0.25 * buffer_manager.GetMaxMemory())) {
		external = true;
	}
	// Use the data that we have to determine which partition size to use during the merge
	if (external && total_heap_size > 0) {
		// If we have variable size data we need to be conservative, as there might be skew
		idx_t max_block_size = 0;
		for (auto &sb : sorted_blocks) {
			idx_t size_in_bytes = sb->SizeInBytes();
			if (size_in_bytes > max_block_size) {
				max_block_size = size_in_bytes;
				block_capacity = sb->Count();
			}
		}
	} else {
		for (auto &sb : sorted_blocks) {
			block_capacity = MaxValue(block_capacity, sb->Count());
		}
	}
	// Unswizzle and pin heap blocks if we can fit everything in memory
	if (!external) {
		for (auto &sb : sorted_blocks) {
			sb->blob_sorting_data->Unswizzle();
			sb->payload_data->Unswizzle();
		}
	}
}

void GlobalSortState::InitializeMergeRound() {
	D_ASSERT(sorted_blocks_temp.empty());
	// If we reverse this list, the blocks that were merged last will be merged first in the next round
	// These are still in memory, therefore this reduces the amount of read/write to disk!
	std::reverse(sorted_blocks.begin(), sorted_blocks.end());
	// Uneven number of blocks - keep one on the side
	if (sorted_blocks.size() % 2 == 1) {
		odd_one_out = move(sorted_blocks.back());
		sorted_blocks.pop_back();
	}
	// Init merge path path indices
	pair_idx = 0;
	num_pairs = sorted_blocks.size() / 2;
	l_start = 0;
	r_start = 0;
	// Allocate room for merge results
	for (idx_t p_idx = 0; p_idx < num_pairs; p_idx++) {
		sorted_blocks_temp.emplace_back();
	}
}

void GlobalSortState::CompleteMergeRound(bool keep_radix_data) {
	sorted_blocks.clear();
	for (auto &sorted_block_vector : sorted_blocks_temp) {
		sorted_blocks.push_back(make_unique<SortedBlock>(buffer_manager, *this));
		sorted_blocks.back()->AppendSortedBlocks(sorted_block_vector);
	}
	sorted_blocks_temp.clear();
	if (odd_one_out) {
		sorted_blocks.push_back(move(odd_one_out));
		odd_one_out = nullptr;
	}
	// Only one block left: Done!
	if (sorted_blocks.size() == 1 && !keep_radix_data) {
		sorted_blocks[0]->radix_sorting_data.clear();
		sorted_blocks[0]->blob_sorting_data = nullptr;
	}
}
void GlobalSortState::Print() {
	PayloadScanner scanner(*this, false);
	DataChunk chunk;
	chunk.Initialize(Allocator::DefaultAllocator(), scanner.GetPayloadTypes());
	for (;;) {
		scanner.Scan(chunk);
		const auto count = chunk.size();
		if (!count) {
			break;
		}
		chunk.Print();
	}
}

} // namespace duckdb






#include <numeric>

namespace duckdb {

SortedData::SortedData(SortedDataType type, const RowLayout &layout, BufferManager &buffer_manager,
                       GlobalSortState &state)
    : type(type), layout(layout), swizzled(false), buffer_manager(buffer_manager), state(state) {
}

idx_t SortedData::Count() {
	idx_t count = std::accumulate(data_blocks.begin(), data_blocks.end(), (idx_t)0,
	                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
	if (!layout.AllConstant() && state.external) {
		D_ASSERT(count == std::accumulate(heap_blocks.begin(), heap_blocks.end(), (idx_t)0,
		                                  [](idx_t a, const RowDataBlock &b) { return a + b.count; }));
	}
	return count;
}

void SortedData::CreateBlock() {
	auto capacity =
	    MaxValue(((idx_t)Storage::BLOCK_SIZE + layout.GetRowWidth() - 1) / layout.GetRowWidth(), state.block_capacity);
	data_blocks.emplace_back(buffer_manager, capacity, layout.GetRowWidth());
	if (!layout.AllConstant() && state.external) {
		heap_blocks.emplace_back(buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1);
		D_ASSERT(data_blocks.size() == heap_blocks.size());
	}
}

unique_ptr<SortedData> SortedData::CreateSlice(idx_t start_block_index, idx_t end_block_index, idx_t end_entry_index) {
	// Add the corresponding blocks to the result
	auto result = make_unique<SortedData>(type, layout, buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->data_blocks.push_back(data_blocks[i]);
		if (!layout.AllConstant() && state.external) {
			result->heap_blocks.push_back(heap_blocks[i]);
		}
	}
	// All of the blocks that come before block with idx = start_block_idx can be reset (other references exist)
	for (idx_t i = 0; i < start_block_index; i++) {
		data_blocks[i].block = nullptr;
		if (!layout.AllConstant() && state.external) {
			heap_blocks[i].block = nullptr;
		}
	}
	// Use start and end entry indices to set the boundaries
	D_ASSERT(end_entry_index <= result->data_blocks.back().count);
	result->data_blocks.back().count = end_entry_index;
	if (!layout.AllConstant() && state.external) {
		result->heap_blocks.back().count = end_entry_index;
	}
	return result;
}

void SortedData::Unswizzle() {
	if (layout.AllConstant() || !swizzled) {
		return;
	}
	for (idx_t i = 0; i < data_blocks.size(); i++) {
		auto &data_block = data_blocks[i];
		auto &heap_block = heap_blocks[i];
		auto data_handle_p = buffer_manager.Pin(data_block.block);
		auto heap_handle_p = buffer_manager.Pin(heap_block.block);
		RowOperations::UnswizzlePointers(layout, data_handle_p.Ptr(), heap_handle_p.Ptr(), data_block.count);
		state.heap_blocks.push_back(move(heap_block));
		state.pinned_blocks.push_back(move(heap_handle_p));
	}
	heap_blocks.clear();
}

SortedBlock::SortedBlock(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), state(state), sort_layout(state.sort_layout),
      payload_layout(state.payload_layout) {
	blob_sorting_data = make_unique<SortedData>(SortedDataType::BLOB, sort_layout.blob_layout, buffer_manager, state);
	payload_data = make_unique<SortedData>(SortedDataType::PAYLOAD, payload_layout, buffer_manager, state);
}

idx_t SortedBlock::Count() const {
	idx_t count = std::accumulate(radix_sorting_data.begin(), radix_sorting_data.end(), 0,
	                              [](idx_t a, const RowDataBlock &b) { return a + b.count; });
	if (!sort_layout.all_constant) {
		D_ASSERT(count == blob_sorting_data->Count());
	}
	D_ASSERT(count == payload_data->Count());
	return count;
}

void SortedBlock::InitializeWrite() {
	CreateBlock();
	if (!sort_layout.all_constant) {
		blob_sorting_data->CreateBlock();
	}
	payload_data->CreateBlock();
}

void SortedBlock::CreateBlock() {
	auto capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + sort_layout.entry_size - 1) / sort_layout.entry_size,
	                         state.block_capacity);
	radix_sorting_data.emplace_back(buffer_manager, capacity, sort_layout.entry_size);
}

void SortedBlock::AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks) {
	D_ASSERT(Count() == 0);
	for (auto &sb : sorted_blocks) {
		for (auto &radix_block : sb->radix_sorting_data) {
			radix_sorting_data.push_back(move(radix_block));
		}
		if (!sort_layout.all_constant) {
			for (auto &blob_block : sb->blob_sorting_data->data_blocks) {
				blob_sorting_data->data_blocks.push_back(move(blob_block));
			}
			for (auto &heap_block : sb->blob_sorting_data->heap_blocks) {
				blob_sorting_data->heap_blocks.push_back(move(heap_block));
			}
		}
		for (auto &payload_data_block : sb->payload_data->data_blocks) {
			payload_data->data_blocks.push_back(move(payload_data_block));
		}
		if (!payload_data->layout.AllConstant()) {
			for (auto &payload_heap_block : sb->payload_data->heap_blocks) {
				payload_data->heap_blocks.push_back(move(payload_heap_block));
			}
		}
	}
}

void SortedBlock::GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index) {
	if (global_idx == Count()) {
		local_block_index = radix_sorting_data.size() - 1;
		local_entry_index = radix_sorting_data.back().count;
		return;
	}
	D_ASSERT(global_idx < Count());
	local_entry_index = global_idx;
	for (local_block_index = 0; local_block_index < radix_sorting_data.size(); local_block_index++) {
		const idx_t &block_count = radix_sorting_data[local_block_index].count;
		if (local_entry_index >= block_count) {
			local_entry_index -= block_count;
		} else {
			break;
		}
	}
	D_ASSERT(local_entry_index < radix_sorting_data[local_block_index].count);
}

unique_ptr<SortedBlock> SortedBlock::CreateSlice(const idx_t start, const idx_t end, idx_t &entry_idx) {
	// Identify blocks/entry indices of this slice
	idx_t start_block_index;
	idx_t start_entry_index;
	GlobalToLocalIndex(start, start_block_index, start_entry_index);
	idx_t end_block_index;
	idx_t end_entry_index;
	GlobalToLocalIndex(end, end_block_index, end_entry_index);
	// Add the corresponding blocks to the result
	auto result = make_unique<SortedBlock>(buffer_manager, state);
	for (idx_t i = start_block_index; i <= end_block_index; i++) {
		result->radix_sorting_data.push_back(radix_sorting_data[i]);
	}
	// Reset all blocks that come before block with idx = start_block_idx (slice holds new reference)
	for (idx_t i = 0; i < start_block_index; i++) {
		radix_sorting_data[i].block = nullptr;
	}
	// Use start and end entry indices to set the boundaries
	entry_idx = start_entry_index;
	D_ASSERT(end_entry_index <= result->radix_sorting_data.back().count);
	result->radix_sorting_data.back().count = end_entry_index;
	// Same for the var size sorting data
	if (!sort_layout.all_constant) {
		result->blob_sorting_data = blob_sorting_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	}
	// And the payload data
	result->payload_data = payload_data->CreateSlice(start_block_index, end_block_index, end_entry_index);
	return result;
}

idx_t SortedBlock::HeapSize() const {
	idx_t result = 0;
	if (!sort_layout.all_constant) {
		for (auto &block : blob_sorting_data->heap_blocks) {
			result += block.capacity;
		}
	}
	if (!payload_layout.AllConstant()) {
		for (auto &block : payload_data->heap_blocks) {
			result += block.capacity;
		}
	}
	return result;
}

idx_t SortedBlock::SizeInBytes() const {
	idx_t bytes = 0;
	for (idx_t i = 0; i < radix_sorting_data.size(); i++) {
		bytes += radix_sorting_data[i].capacity * sort_layout.entry_size;
		if (!sort_layout.all_constant) {
			bytes += blob_sorting_data->data_blocks[i].capacity * sort_layout.blob_layout.GetRowWidth();
			bytes += blob_sorting_data->heap_blocks[i].capacity;
		}
		bytes += payload_data->data_blocks[i].capacity * payload_layout.GetRowWidth();
		if (!payload_layout.AllConstant()) {
			bytes += payload_data->heap_blocks[i].capacity;
		}
	}
	return bytes;
}

SBScanState::SBScanState(BufferManager &buffer_manager, GlobalSortState &state)
    : buffer_manager(buffer_manager), sort_layout(state.sort_layout), state(state), block_idx(0), entry_idx(0) {
}

void SBScanState::PinRadix(idx_t block_idx_to) {
	auto &radix_sorting_data = sb->radix_sorting_data;
	D_ASSERT(block_idx_to < radix_sorting_data.size());
	auto &block = radix_sorting_data[block_idx_to];
	if (!radix_handle.IsValid() || radix_handle.GetBlockId() != block.block->BlockId()) {
		radix_handle = buffer_manager.Pin(block.block);
	}
}

void SBScanState::PinData(SortedData &sd) {
	D_ASSERT(block_idx < sd.data_blocks.size());
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;

	auto &data_block = sd.data_blocks[block_idx];
	if (!data_handle.IsValid() || data_handle.GetBlockId() != data_block.block->BlockId()) {
		data_handle = buffer_manager.Pin(data_block.block);
	}
	if (sd.layout.AllConstant() || !state.external) {
		return;
	}
	auto &heap_block = sd.heap_blocks[block_idx];
	if (!heap_handle.IsValid() || heap_handle.GetBlockId() != heap_block.block->BlockId()) {
		heap_handle = buffer_manager.Pin(heap_block.block);
	}
}

data_ptr_t SBScanState::RadixPtr() const {
	return radix_handle.Ptr() + entry_idx * sort_layout.entry_size;
}

data_ptr_t SBScanState::DataPtr(SortedData &sd) const {
	auto &data_handle = sd.type == SortedDataType::BLOB ? blob_sorting_data_handle : payload_data_handle;
	D_ASSERT(sd.data_blocks[block_idx].block->Readers() != 0 &&
	         data_handle.GetBlockId() == sd.data_blocks[block_idx].block->BlockId());
	return data_handle.Ptr() + entry_idx * sd.layout.GetRowWidth();
}

data_ptr_t SBScanState::HeapPtr(SortedData &sd) const {
	return BaseHeapPtr(sd) + Load<idx_t>(DataPtr(sd) + sd.layout.GetHeapPointerOffset());
}

data_ptr_t SBScanState::BaseHeapPtr(SortedData &sd) const {
	auto &heap_handle = sd.type == SortedDataType::BLOB ? blob_sorting_heap_handle : payload_heap_handle;
	D_ASSERT(!sd.layout.AllConstant() && state.external);
	D_ASSERT(sd.heap_blocks[block_idx].block->Readers() != 0 &&
	         heap_handle.GetBlockId() == sd.heap_blocks[block_idx].block->BlockId());
	return heap_handle.Ptr();
}

idx_t SBScanState::Remaining() const {
	const auto &blocks = sb->radix_sorting_data;
	idx_t remaining = 0;
	if (block_idx < blocks.size()) {
		remaining += blocks[block_idx].count - entry_idx;
		for (idx_t i = block_idx + 1; i < blocks.size(); i++) {
			remaining += blocks[i].count;
		}
	}
	return remaining;
}

void SBScanState::SetIndices(idx_t block_idx_to, idx_t entry_idx_to) {
	block_idx = block_idx_to;
	entry_idx = entry_idx_to;
}

PayloadScanner::PayloadScanner(SortedData &sorted_data, GlobalSortState &global_sort_state, bool flush_p)
    : sorted_data(sorted_data), read_state(global_sort_state.buffer_manager, global_sort_state),
      total_count(sorted_data.Count()), global_sort_state(global_sort_state), total_scanned(0), flush(flush_p) {
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, bool flush_p)
    : PayloadScanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state, flush_p) {
}

PayloadScanner::PayloadScanner(GlobalSortState &global_sort_state, idx_t block_idx)
    : sorted_data(*global_sort_state.sorted_blocks[0]->payload_data),
      read_state(global_sort_state.buffer_manager, global_sort_state),
      total_count(sorted_data.data_blocks[block_idx].count), global_sort_state(global_sort_state), total_scanned(0),
      flush(false) {
	read_state.SetIndices(block_idx, 0);
}

void PayloadScanner::Scan(DataChunk &chunk) {
	auto count = MinValue((idx_t)STANDARD_VECTOR_SIZE, total_count - total_scanned);
	if (count == 0) {
		chunk.SetCardinality(count);
		return;
	}
	// Eagerly delete references to blocks that we've passed
	if (flush) {
		for (idx_t i = 0; i < read_state.block_idx; i++) {
			sorted_data.data_blocks[i].block = nullptr;
		}
	}
	const idx_t &row_width = sorted_data.layout.GetRowWidth();
	// Set up a batch of pointers to scan data from
	idx_t scanned = 0;
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	while (scanned < count) {
		read_state.PinData(sorted_data);
		auto &data_block = sorted_data.data_blocks[read_state.block_idx];
		idx_t next = MinValue(data_block.count - read_state.entry_idx, count - scanned);
		const data_ptr_t data_ptr = read_state.payload_data_handle.Ptr() + read_state.entry_idx * row_width;
		// Set up the next pointers
		data_ptr_t row_ptr = data_ptr;
		for (idx_t i = 0; i < next; i++) {
			data_pointers[scanned + i] = row_ptr;
			row_ptr += row_width;
		}
		// Unswizzle the offsets back to pointers (if needed)
		if (!sorted_data.layout.AllConstant() && global_sort_state.external) {
			RowOperations::UnswizzlePointers(sorted_data.layout, data_ptr, read_state.payload_heap_handle.Ptr(), next);
		}
		// Update state indices
		read_state.entry_idx += next;
		if (read_state.entry_idx == data_block.count) {
			read_state.block_idx++;
			read_state.entry_idx = 0;
		}
		scanned += next;
	}
	D_ASSERT(scanned == count);
	// Deserialize the payload data
	for (idx_t col_idx = 0; col_idx < sorted_data.layout.ColumnCount(); col_idx++) {
		const auto col_offset = sorted_data.layout.GetOffsets()[col_idx];
		RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), chunk.data[col_idx],
		                      *FlatVector::IncrementalSelectionVector(), count, col_offset, col_idx);
	}
	chunk.SetCardinality(count);
	chunk.Verify();
	total_scanned += scanned;
}

} // namespace duckdb






#include <algorithm>
#include <cctype>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdarg.h>
#include <string.h>

namespace duckdb {

bool StringUtil::Contains(const string &haystack, const string &needle) {
	return (haystack.find(needle) != string::npos);
}

void StringUtil::LTrim(string &str) {
	auto it = str.begin();
	while (CharacterIsSpace(*it)) {
		it++;
	}
	str.erase(str.begin(), it);
}

// Remove trailing ' ', '\f', '\n', '\r', '\t', '\v'
void StringUtil::RTrim(string &str) {
	str.erase(find_if(str.rbegin(), str.rend(), [](int ch) { return ch > 0 && !CharacterIsSpace(ch); }).base(),
	          str.end());
}

void StringUtil::Trim(string &str) {
	StringUtil::LTrim(str);
	StringUtil::RTrim(str);
}

bool StringUtil::StartsWith(string str, string prefix) {
	if (prefix.size() > str.size()) {
		return false;
	}
	return equal(prefix.begin(), prefix.end(), str.begin());
}

bool StringUtil::EndsWith(const string &str, const string &suffix) {
	if (suffix.size() > str.size()) {
		return false;
	}
	return equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

string StringUtil::Repeat(const string &str, idx_t n) {
	std::ostringstream os;
	for (idx_t i = 0; i < n; i++) {
		os << str;
	}
	return (os.str());
}

vector<string> StringUtil::Split(const string &str, char delimiter) {
	std::stringstream ss(str);
	vector<string> lines;
	string temp;
	while (getline(ss, temp, delimiter)) {
		lines.push_back(temp);
	}
	return (lines);
}

namespace string_util_internal {

inline void SkipSpaces(const string &str, idx_t &index) {
	while (index < str.size() && std::isspace(str[index])) {
		index++;
	}
}

inline void ConsumeLetter(const string &str, idx_t &index, char expected) {
	if (index >= str.size() || str[index] != expected) {
		throw ParserException("Invalid quoted list: %s", str);
	}

	index++;
}

template <typename F>
inline void TakeWhile(const string &str, idx_t &index, const F &cond, string &taker) {
	while (index < str.size() && cond(str[index])) {
		taker.push_back(str[index]);
		index++;
	}
}

inline string TakePossiblyQuotedItem(const string &str, idx_t &index, char delimiter, char quote) {
	string entry;

	if (str[index] == quote) {
		index++;
		TakeWhile(
		    str, index, [quote](char c) { return c != quote; }, entry);
		ConsumeLetter(str, index, quote);
	} else {
		TakeWhile(
		    str, index, [delimiter, quote](char c) { return c != delimiter && c != quote && !std::isspace(c); }, entry);
	}

	return entry;
}

} // namespace string_util_internal

vector<string> StringUtil::SplitWithQuote(const string &str, char delimiter, char quote) {
	vector<string> entries;
	idx_t i = 0;

	string_util_internal::SkipSpaces(str, i);
	while (i < str.size()) {
		if (!entries.empty()) {
			string_util_internal::ConsumeLetter(str, i, delimiter);
		}

		entries.emplace_back(string_util_internal::TakePossiblyQuotedItem(str, i, delimiter, quote));
		string_util_internal::SkipSpaces(str, i);
	}

	return entries;
}

string StringUtil::Join(const vector<string> &input, const string &separator) {
	return StringUtil::Join(input, input.size(), separator, [](const string &s) { return s; });
}

string StringUtil::BytesToHumanReadableString(idx_t bytes) {
	string db_size;
	auto kilobytes = bytes / 1000;
	auto megabytes = kilobytes / 1000;
	kilobytes -= megabytes * 1000;
	auto gigabytes = megabytes / 1000;
	megabytes -= gigabytes * 1000;
	auto terabytes = gigabytes / 1000;
	gigabytes -= terabytes * 1000;
	if (terabytes > 0) {
		return to_string(terabytes) + "." + to_string(gigabytes / 100) + "TB";
	} else if (gigabytes > 0) {
		return to_string(gigabytes) + "." + to_string(megabytes / 100) + "GB";
	} else if (megabytes > 0) {
		return to_string(megabytes) + "." + to_string(kilobytes / 100) + "MB";
	} else if (kilobytes > 0) {
		return to_string(kilobytes) + "KB";
	} else {
		return to_string(bytes) + " bytes";
	}
}

string StringUtil::Upper(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::toupper(c); });
	return (copy);
}

string StringUtil::Lower(const string &str) {
	string copy(str);
	transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::tolower(c); });
	return (copy);
}

vector<string> StringUtil::Split(const string &input, const string &split) {
	vector<string> splits;

	idx_t last = 0;
	idx_t input_len = input.size();
	idx_t split_len = split.size();
	while (last <= input_len) {
		idx_t next = input.find(split, last);
		if (next == string::npos) {
			next = input_len;
		}

		// Push the substring [last, next) on to splits
		string substr = input.substr(last, next - last);
		if (substr.empty() == false) {
			splits.push_back(substr);
		}
		last = next + split_len;
	}
	return splits;
}

string StringUtil::Replace(string source, const string &from, const string &to) {
	idx_t start_pos = 0;
	while ((start_pos = source.find(from, start_pos)) != string::npos) {
		source.replace(start_pos, from.length(), to);
		start_pos += to.length(); // In case 'to' contains 'from', like
		                          // replacing 'x' with 'yx'
	}
	return source;
}

vector<string> StringUtil::TopNStrings(vector<pair<string, idx_t>> scores, idx_t n, idx_t threshold) {
	if (scores.empty()) {
		return vector<string>();
	}
	sort(scores.begin(), scores.end(),
	     [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) -> bool { return a.second < b.second; });
	vector<string> result;
	result.push_back(scores[0].first);
	for (idx_t i = 1; i < MinValue<idx_t>(scores.size(), n); i++) {
		if (scores[i].second > threshold) {
			break;
		}
		result.push_back(scores[i].first);
	}
	return result;
}

struct LevenshteinArray {
	LevenshteinArray(idx_t len1, idx_t len2) : len1(len1) {
		dist = unique_ptr<idx_t[]>(new idx_t[len1 * len2]);
	}

	idx_t &Score(idx_t i, idx_t j) {
		return dist[GetIndex(i, j)];
	}

private:
	idx_t len1;
	unique_ptr<idx_t[]> dist;

	idx_t GetIndex(idx_t i, idx_t j) {
		return j * len1 + i;
	}
};

// adapted from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C++
idx_t StringUtil::LevenshteinDistance(const string &s1_p, const string &s2_p) {
	auto s1 = StringUtil::Lower(s1_p);
	auto s2 = StringUtil::Lower(s2_p);
	idx_t len1 = s1.size();
	idx_t len2 = s2.size();
	if (len1 == 0) {
		return len2;
	}
	if (len2 == 0) {
		return len1;
	}
	LevenshteinArray array(len1 + 1, len2 + 1);
	array.Score(0, 0) = 0;
	for (idx_t i = 0; i <= len1; i++) {
		array.Score(i, 0) = i;
	}
	for (idx_t j = 0; j <= len2; j++) {
		array.Score(0, j) = j;
	}
	for (idx_t i = 1; i <= len1; i++) {
		for (idx_t j = 1; j <= len2; j++) {
			// d[i][j] = std::min({ d[i - 1][j] + 1,
			//                      d[i][j - 1] + 1,
			//                      d[i - 1][j - 1] + (s1[i - 1] == s2[j - 1] ? 0 : 1) });
			int equal = s1[i - 1] == s2[j - 1] ? 0 : 1;
			idx_t adjacent_score1 = array.Score(i - 1, j) + 1;
			idx_t adjacent_score2 = array.Score(i, j - 1) + 1;
			idx_t adjacent_score3 = array.Score(i - 1, j - 1) + equal;

			idx_t t = MinValue<idx_t>(adjacent_score1, adjacent_score2);
			array.Score(i, j) = MinValue<idx_t>(t, adjacent_score3);
		}
	}
	return array.Score(len1, len2);
}

vector<string> StringUtil::TopNLevenshtein(const vector<string> &strings, const string &target, idx_t n,
                                           idx_t threshold) {
	vector<pair<string, idx_t>> scores;
	scores.reserve(strings.size());
	for (auto &str : strings) {
		scores.emplace_back(str, LevenshteinDistance(str, target));
	}
	return TopNStrings(scores, n, threshold);
}

string StringUtil::CandidatesMessage(const vector<string> &candidates, const string &candidate) {
	string result_str;
	if (!candidates.empty()) {
		result_str = "\n" + candidate + ": ";
		for (idx_t i = 0; i < candidates.size(); i++) {
			if (i > 0) {
				result_str += ", ";
			}
			result_str += "\"" + candidates[i] + "\"";
		}
	}
	return result_str;
}

string StringUtil::CandidatesErrorMessage(const vector<string> &strings, const string &target,
                                          const string &message_prefix, idx_t n) {
	auto closest_strings = StringUtil::TopNLevenshtein(strings, target, n);
	return StringUtil::CandidatesMessage(closest_strings, message_prefix);
}

} // namespace duckdb












#include <sstream>

namespace duckdb {

RenderTree::RenderTree(idx_t width_p, idx_t height_p) : width(width_p), height(height_p) {
	nodes = unique_ptr<unique_ptr<RenderTreeNode>[]>(new unique_ptr<RenderTreeNode>[(width + 1) * (height + 1)]);
}

RenderTreeNode *RenderTree::GetNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return nullptr;
	}
	return nodes[GetPosition(x, y)].get();
}

bool RenderTree::HasNode(idx_t x, idx_t y) {
	if (x >= width || y >= height) {
		return false;
	}
	return nodes[GetPosition(x, y)].get() != nullptr;
}

idx_t RenderTree::GetPosition(idx_t x, idx_t y) {
	return y * width + x;
}

void RenderTree::SetNode(idx_t x, idx_t y, unique_ptr<RenderTreeNode> node) {
	nodes[GetPosition(x, y)] = move(node);
}

void TreeRenderer::RenderTopLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x < root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LTCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (y == 0) {
				// top level node: no node above this one
				ss << config.HORIZONTAL;
			} else {
				// render connection to node above this one
				ss << config.DMIDDLE;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RTCORNER;
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

void TreeRenderer::RenderBottomLayer(RenderTree &root, std::ostream &ss, idx_t y) {
	for (idx_t x = 0; x <= root.width; x++) {
		if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
			break;
		}
		if (root.HasNode(x, y)) {
			ss << config.LDCORNER;
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			if (root.HasNode(x, y + 1)) {
				// node below this one: connect to that one
				ss << config.TMIDDLE;
			} else {
				// no node below this one: end the box
				ss << config.HORIZONTAL;
			}
			ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2 - 1);
			ss << config.RDCORNER;
		} else if (root.HasNode(x, y + 1)) {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
			ss << config.VERTICAL;
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
		} else {
			ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
		}
	}
	ss << std::endl;
}

string AdjustTextForRendering(string source, idx_t max_render_width) {
	idx_t cpos = 0;
	idx_t render_width = 0;
	vector<pair<idx_t, idx_t>> render_widths;
	while (cpos < source.size()) {
		idx_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		render_width += char_render_width;
		render_widths.emplace_back(cpos, render_width);
		if (render_width > max_render_width) {
			break;
		}
	}
	if (render_width > max_render_width) {
		// need to find a position to truncate
		for (idx_t pos = render_widths.size(); pos > 0; pos--) {
			if (render_widths[pos - 1].second < max_render_width - 4) {
				return source.substr(0, render_widths[pos - 1].first) + "..." +
				       string(max_render_width - render_widths[pos - 1].second - 3, ' ');
			}
		}
		source = "...";
	}
	// need to pad with spaces
	idx_t total_spaces = max_render_width - render_width;
	idx_t half_spaces = total_spaces / 2;
	idx_t extra_left_space = total_spaces % 2 == 0 ? 0 : 1;
	return string(half_spaces + extra_left_space, ' ') + source + string(half_spaces, ' ');
}

static bool NodeHasMultipleChildren(RenderTree &root, idx_t x, idx_t y) {
	for (; x < root.width && !root.HasNode(x + 1, y); x++) {
		if (root.HasNode(x + 1, y + 1)) {
			return true;
		}
	}
	return false;
}

void TreeRenderer::RenderBoxContent(RenderTree &root, std::ostream &ss, idx_t y) {
	// we first need to figure out how high our boxes are going to be
	vector<vector<string>> extra_info;
	idx_t extra_height = 0;
	extra_info.resize(root.width);
	for (idx_t x = 0; x < root.width; x++) {
		auto node = root.GetNode(x, y);
		if (node) {
			SplitUpExtraInfo(node->extra_text, extra_info[x]);
			if (extra_info[x].size() > extra_height) {
				extra_height = extra_info[x].size();
			}
		}
	}
	extra_height = MinValue<idx_t>(extra_height, config.MAX_EXTRA_LINES);
	idx_t halfway_point = (extra_height + 1) / 2;
	// now we render the actual node
	for (idx_t render_y = 0; render_y <= extra_height; render_y++) {
		for (idx_t x = 0; x < root.width; x++) {
			if (x * config.NODE_RENDER_WIDTH >= config.MAXIMUM_RENDER_WIDTH) {
				break;
			}
			auto node = root.GetNode(x, y);
			if (!node) {
				if (render_y == halfway_point) {
					bool has_child_to_the_right = NodeHasMultipleChildren(root, x, y);
					if (root.HasNode(x, y + 1)) {
						// node right below this one
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						ss << config.RTCORNER;
						if (has_child_to_the_right) {
							// but we have another child to the right! keep rendering the line
							ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH / 2);
						} else {
							// only a child below this one: fill the rest with spaces
							ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						}
					} else if (has_child_to_the_right) {
						// child to the right, but no child right below this one: render a full line
						ss << StringUtil::Repeat(config.HORIZONTAL, config.NODE_RENDER_WIDTH);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else if (render_y >= halfway_point) {
					if (root.HasNode(x, y + 1)) {
						// we have a node below this empty spot: render a vertical line
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
						ss << config.VERTICAL;
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH / 2);
					} else {
						// empty spot: render spaces
						ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
					}
				} else {
					// empty spot: render spaces
					ss << StringUtil::Repeat(" ", config.NODE_RENDER_WIDTH);
				}
			} else {
				ss << config.VERTICAL;
				// figure out what to render
				string render_text;
				if (render_y == 0) {
					render_text = node->name;
				} else {
					if (render_y <= extra_info[x].size()) {
						render_text = extra_info[x][render_y - 1];
					}
				}
				render_text = AdjustTextForRendering(render_text, config.NODE_RENDER_WIDTH - 2);
				ss << render_text;

				if (render_y == halfway_point && NodeHasMultipleChildren(root, x, y)) {
					ss << config.LMIDDLE;
				} else {
					ss << config.VERTICAL;
				}
			}
		}
		ss << std::endl;
	}
}

string TreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const QueryProfiler::TreeNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string TreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void TreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const QueryProfiler::TreeNode &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = CreateTree(op);
	ToStream(*tree, ss);
}

void TreeRenderer::ToStream(RenderTree &root, std::ostream &ss) {
	while (root.width * config.NODE_RENDER_WIDTH > config.MAXIMUM_RENDER_WIDTH) {
		if (config.NODE_RENDER_WIDTH - 2 < config.MINIMUM_RENDER_WIDTH) {
			break;
		}
		config.NODE_RENDER_WIDTH -= 2;
	}

	for (idx_t y = 0; y < root.height; y++) {
		// start by rendering the top layer
		RenderTopLayer(root, ss, y);
		// now we render the content of the boxes
		RenderBoxContent(root, ss, y);
		// render the bottom layer of each of the boxes
		RenderBottomLayer(root, ss, y);
	}
}

bool TreeRenderer::CanSplitOnThisChar(char l) {
	return (l < '0' || (l > '9' && l < 'A') || (l > 'Z' && l < 'a')) && l != '_';
}

bool TreeRenderer::IsPadding(char l) {
	return l == ' ' || l == '\t' || l == '\n' || l == '\r';
}

string TreeRenderer::RemovePadding(string l) {
	idx_t start = 0, end = l.size();
	while (start < l.size() && IsPadding(l[start])) {
		start++;
	}
	while (end > 0 && IsPadding(l[end - 1])) {
		end--;
	}
	return l.substr(start, end - start);
}

void TreeRenderer::SplitStringBuffer(const string &source, vector<string> &result) {
	D_ASSERT(Utf8Proc::IsValid(source.c_str(), source.size()));
	idx_t max_line_render_size = config.NODE_RENDER_WIDTH - 2;
	// utf8 in prompt, get render width
	idx_t cpos = 0;
	idx_t start_pos = 0;
	idx_t render_width = 0;
	idx_t last_possible_split = 0;
	while (cpos < source.size()) {
		// check if we can split on this character
		if (CanSplitOnThisChar(source[cpos])) {
			last_possible_split = cpos;
		}
		size_t char_render_width = Utf8Proc::RenderWidth(source.c_str(), source.size(), cpos);
		idx_t next_cpos = Utf8Proc::NextGraphemeCluster(source.c_str(), source.size(), cpos);
		if (render_width + char_render_width > max_line_render_size) {
			if (last_possible_split <= start_pos + 8) {
				last_possible_split = cpos;
			}
			result.push_back(source.substr(start_pos, last_possible_split - start_pos));
			start_pos = last_possible_split;
			cpos = last_possible_split;
			render_width = 0;
		}
		cpos = next_cpos;
		render_width += char_render_width;
	}
	if (source.size() > start_pos) {
		result.push_back(source.substr(start_pos, source.size() - start_pos));
	}
}

void TreeRenderer::SplitUpExtraInfo(const string &extra_info, vector<string> &result) {
	if (extra_info.empty()) {
		return;
	}
	if (!Utf8Proc::IsValid(extra_info.c_str(), extra_info.size())) {
		return;
	}
	auto splits = StringUtil::Split(extra_info, "\n");
	if (!splits.empty() && splits[0] != "[INFOSEPARATOR]") {
		result.push_back(ExtraInfoSeparator());
	}
	for (auto &split : splits) {
		if (split == "[INFOSEPARATOR]") {
			result.push_back(ExtraInfoSeparator());
			continue;
		}
		string str = RemovePadding(split);
		if (str.empty()) {
			continue;
		}
		SplitStringBuffer(str, result);
	}
}

string TreeRenderer::ExtraInfoSeparator() {
	return StringUtil::Repeat(string(config.HORIZONTAL) + " ", (config.NODE_RENDER_WIDTH - 7) / 2);
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateRenderNode(string name, string extra_info) {
	auto result = make_unique<RenderTreeNode>();
	result->name = move(name);
	result->extra_text = move(extra_info);
	return result;
}

class TreeChildrenIterator {
public:
	template <class T>
	static bool HasChildren(const T &op) {
		return !op.children.empty();
	}
	template <class T>
	static void Iterate(const T &op, const std::function<void(const T &child)> &callback) {
		for (auto &child : op.children) {
			callback(*child);
		}
	}
};

template <>
bool TreeChildrenIterator::HasChildren(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::DELIM_JOIN) {
		return true;
	}
	return !op.children.empty();
}
template <>
void TreeChildrenIterator::Iterate(const PhysicalOperator &op,
                                   const std::function<void(const PhysicalOperator &child)> &callback) {
	for (auto &child : op.children) {
		callback(*child);
	}
	if (op.type == PhysicalOperatorType::DELIM_JOIN) {
		auto &delim = (PhysicalDelimJoin &)op;
		callback(*delim.join);
	}
}

struct PipelineRenderNode {
	explicit PipelineRenderNode(PhysicalOperator &op) : op(op) {
	}

	PhysicalOperator &op;
	unique_ptr<PipelineRenderNode> child;
};

template <>
bool TreeChildrenIterator::HasChildren(const PipelineRenderNode &op) {
	return op.child.get();
}

template <>
void TreeChildrenIterator::Iterate(const PipelineRenderNode &op,
                                   const std::function<void(const PipelineRenderNode &child)> &callback) {
	if (op.child) {
		callback(*op.child);
	}
}

template <class T>
static void GetTreeWidthHeight(const T &op, idx_t &width, idx_t &height) {
	if (!TreeChildrenIterator::HasChildren(op)) {
		width = 1;
		height = 1;
		return;
	}
	width = 0;
	height = 0;

	TreeChildrenIterator::Iterate<T>(op, [&](const T &child) {
		idx_t child_width, child_height;
		GetTreeWidthHeight<T>(child, child_width, child_height);
		width += child_width;
		height = MaxValue<idx_t>(height, child_height);
	});
	height++;
}

template <class T>
idx_t TreeRenderer::CreateRenderTreeRecursive(RenderTree &result, const T &op, idx_t x, idx_t y) {
	auto node = TreeRenderer::CreateNode(op);
	result.SetNode(x, y, move(node));

	if (!TreeChildrenIterator::HasChildren(op)) {
		return 1;
	}
	idx_t width = 0;
	// render the children of this node
	TreeChildrenIterator::Iterate<T>(
	    op, [&](const T &child) { width += CreateRenderTreeRecursive<T>(result, child, x + width, y + 1); });
	return width;
}

template <class T>
unique_ptr<RenderTree> TreeRenderer::CreateRenderTree(const T &op) {
	idx_t width, height;
	GetTreeWidthHeight<T>(op, width, height);

	auto result = make_unique<RenderTree>(width, height);

	// now fill in the tree
	CreateRenderTreeRecursive<T>(*result, op, 0, 0);
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const LogicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PhysicalOperator &op) {
	return CreateRenderNode(op.GetName(), op.ParamsToString());
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const PipelineRenderNode &op) {
	return CreateNode(op.op);
}

string TreeRenderer::ExtractExpressionsRecursive(ExpressionInfo &state) {
	string result = "\n[INFOSEPARATOR]";
	result += "\n" + state.function_name;
	result += "\n" + StringUtil::Format("%.9f", double(state.function_time));
	if (state.children.empty()) {
		return result;
	}
	// render the children of this node
	for (auto &child : state.children) {
		result += ExtractExpressionsRecursive(*child);
	}
	return result;
}

unique_ptr<RenderTreeNode> TreeRenderer::CreateNode(const QueryProfiler::TreeNode &op) {
	auto result = TreeRenderer::CreateRenderNode(op.name, op.extra_info);
	result->extra_text += "\n[INFOSEPARATOR]";
	result->extra_text += "\n" + to_string(op.info.elements);
	string timing = StringUtil::Format("%.2f", op.info.time);
	result->extra_text += "\n(" + timing + "s)";
	if (config.detailed) {
		for (auto &info : op.info.executors_info) {
			if (!info) {
				continue;
			}
			for (auto &executor_info : info->roots) {
				string sample_count = to_string(executor_info->sample_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_count: " + sample_count;
				string sample_tuples_count = to_string(executor_info->sample_tuples_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\nsample_tuples_count: " + sample_tuples_count;
				string total_count = to_string(executor_info->total_count);
				result->extra_text += "\n[INFOSEPARATOR]";
				result->extra_text += "\ntotal_count: " + total_count;
				for (auto &state : executor_info->root->children) {
					result->extra_text += ExtractExpressionsRecursive(*state);
				}
			}
		}
	}
	return result;
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const LogicalOperator &op) {
	return CreateRenderTree<LogicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const PhysicalOperator &op) {
	return CreateRenderTree<PhysicalOperator>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const QueryProfiler::TreeNode &op) {
	return CreateRenderTree<QueryProfiler::TreeNode>(op);
}

unique_ptr<RenderTree> TreeRenderer::CreateTree(const Pipeline &op) {
	auto operators = op.GetOperators();
	D_ASSERT(!operators.empty());
	unique_ptr<PipelineRenderNode> node;
	for (auto &op : operators) {
		auto new_node = make_unique<PipelineRenderNode>(*op);
		new_node->child = move(node);
		node = move(new_node);
	}
	return CreateRenderTree<PipelineRenderNode>(*node);
}

} // namespace duckdb



namespace duckdb {

BatchedChunkCollection::BatchedChunkCollection(Allocator &allocator) : allocator(allocator) {
}

void BatchedChunkCollection::Append(DataChunk &input, idx_t batch_index) {
	D_ASSERT(batch_index != DConstants::INVALID_INDEX);
	auto entry = data.find(batch_index);
	ChunkCollection *collection;
	if (entry == data.end()) {
		auto new_collection = make_unique<ChunkCollection>(allocator);
		collection = new_collection.get();
		data.insert(make_pair(batch_index, move(new_collection)));
	} else {
		collection = entry->second.get();
	}
	collection->Append(input);
}

void BatchedChunkCollection::Merge(BatchedChunkCollection &other) {
	for (auto &entry : other.data) {
		if (data.find(entry.first) != data.end()) {
			throw InternalException(
			    "BatchChunkCollection::Merge error - batch index %d is present in both collections. This occurs when "
			    "batch indexes are not uniquely distributed over threads",
			    entry.first);
		}
		data[entry.first] = move(entry.second);
	}
	other.data.clear();
}

void BatchedChunkCollection::InitializeScan(BatchedChunkScanState &state) {
	state.iterator = data.begin();
	state.chunk_index = 0;
}

void BatchedChunkCollection::Scan(BatchedChunkScanState &state, DataChunk &output) {
	while (state.iterator != data.end()) {
		// check if there is a chunk remaining in this collection
		auto collection = state.iterator->second.get();
		if (state.chunk_index < collection->ChunkCount()) {
			// there is! increment the chunk count
			output.Reference(collection->GetChunk(state.chunk_index));
			state.chunk_index++;
			return;
		}
		// there isn't! move to the next collection
		state.iterator++;
		state.chunk_index = 0;
	}
}

string BatchedChunkCollection::ToString() const {
	string result;
	result += "Batched Chunk Collection\n";
	for (auto &entry : data) {
		result += "Batch Index - " + to_string(entry.first) + "\n";
		result += entry.second->ToString() + "\n\n";
	}
	return result;
}

void BatchedChunkCollection::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb







namespace duckdb {

constexpr const char *Blob::HEX_TABLE;
const int Blob::HEX_MAP[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
    -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

idx_t Blob::GetStringSize(string_t blob) {
	auto data = (const_data_ptr_t)blob.GetDataUnsafe();
	auto len = blob.GetSize();
	idx_t str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] >= 32 && data[i] <= 127 && data[i] != '\\') {
			// ascii characters are rendered as-is
			str_len++;
		} else {
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			str_len += 4;
		}
	}
	return str_len;
}

void Blob::ToString(string_t blob, char *output) {
	auto data = (const_data_ptr_t)blob.GetDataUnsafe();
	auto len = blob.GetSize();
	idx_t str_idx = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] >= 32 && data[i] <= 127 && data[i] != '\\') {
			// ascii characters are rendered as-is
			output[str_idx++] = data[i];
		} else {
			auto byte_a = data[i] >> 4;
			auto byte_b = data[i] & 0x0F;
			D_ASSERT(byte_a >= 0 && byte_a < 16);
			D_ASSERT(byte_b >= 0 && byte_b < 16);
			// non-ascii characters are rendered as hexadecimal (e.g. \x00)
			output[str_idx++] = '\\';
			output[str_idx++] = 'x';
			output[str_idx++] = Blob::HEX_TABLE[byte_a];
			output[str_idx++] = Blob::HEX_TABLE[byte_b];
		}
	}
	D_ASSERT(str_idx == GetStringSize(blob));
}

string Blob::ToString(string_t blob) {
	auto str_len = GetStringSize(blob);
	auto buffer = std::unique_ptr<char[]>(new char[str_len]);
	Blob::ToString(blob, buffer.get());
	return string(buffer.get(), str_len);
}

bool Blob::TryGetBlobSize(string_t str, idx_t &str_len, string *error_message) {
	auto data = (const_data_ptr_t)str.GetDataUnsafe();
	auto len = str.GetSize();
	str_len = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			if (i + 3 >= len) {
				string error = "Invalid hex escape code encountered in string -> blob conversion: "
				               "unterminated escape code at end of blob";
				HandleCastError::AssignError(error, error_message);
				return false;
			}
			if (data[i + 1] != 'x' || Blob::HEX_MAP[data[i + 2]] < 0 || Blob::HEX_MAP[data[i + 3]] < 0) {
				string error =
				    StringUtil::Format("Invalid hex escape code encountered in string -> blob conversion: %s",
				                       string((char *)data + i, 4));
				HandleCastError::AssignError(error, error_message);
				return false;
			}
			str_len++;
			i += 3;
		} else if (data[i] >= 32 && data[i] <= 127) {
			str_len++;
		} else {
			string error = "Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters "
			               "must be escaped with hex codes (e.g. \\xAA)";
			HandleCastError::AssignError(error, error_message);
			return false;
		}
	}
	return true;
}

idx_t Blob::GetBlobSize(string_t str) {
	string error_message;
	idx_t str_len;
	if (!Blob::TryGetBlobSize(str, str_len, &error_message)) {
		throw ConversionException(error_message);
	}
	return str_len;
}

void Blob::ToBlob(string_t str, data_ptr_t output) {
	auto data = (const_data_ptr_t)str.GetDataUnsafe();
	auto len = str.GetSize();
	idx_t blob_idx = 0;
	for (idx_t i = 0; i < len; i++) {
		if (data[i] == '\\') {
			int byte_a = Blob::HEX_MAP[data[i + 2]];
			int byte_b = Blob::HEX_MAP[data[i + 3]];
			D_ASSERT(i + 3 < len);
			D_ASSERT(byte_a >= 0 && byte_b >= 0);
			D_ASSERT(data[i + 1] == 'x');
			output[blob_idx++] = (byte_a << 4) + byte_b;
			i += 3;
		} else if (data[i] >= 32 && data[i] <= 127) {
			output[blob_idx++] = data_t(data[i]);
		} else {
			throw ConversionException("Invalid byte encountered in STRING -> BLOB conversion. All non-ascii characters "
			                          "must be escaped with hex codes (e.g. \\xAA)");
		}
	}
	D_ASSERT(blob_idx == GetBlobSize(str));
}

string Blob::ToBlob(string_t str) {
	auto blob_len = GetBlobSize(str);
	auto buffer = std::unique_ptr<char[]>(new char[blob_len]);
	Blob::ToBlob(str, (data_ptr_t)buffer.get());
	return string(buffer.get(), blob_len);
}

// base64 functions are adapted from https://gist.github.com/tomykaira/f0fd86b6c73063283afe550bc5d77594
idx_t Blob::ToBase64Size(string_t blob) {
	// every 4 characters in base64 encode 3 bytes, plus (potential) padding at the end
	auto input_size = blob.GetSize();
	return ((input_size + 2) / 3) * 4;
}

void Blob::ToBase64(string_t blob, char *output) {
	auto input_data = (const_data_ptr_t)blob.GetDataUnsafe();
	auto input_size = blob.GetSize();
	idx_t out_idx = 0;
	idx_t i;
	// convert the bulk of the string to base64
	// this happens in steps of 3 bytes -> 4 output bytes
	for (i = 0; i + 2 < input_size; i += 3) {
		output[out_idx++] = Blob::BASE64_MAP[(input_data[i] >> 2) & 0x3F];
		output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4) | ((input_data[i + 1] & 0xF0) >> 4)];
		output[out_idx++] = Blob::BASE64_MAP[((input_data[i + 1] & 0xF) << 2) | ((input_data[i + 2] & 0xC0) >> 6)];
		output[out_idx++] = Blob::BASE64_MAP[input_data[i + 2] & 0x3F];
	}

	if (i < input_size) {
		// there are one or two bytes left over: we have to insert padding
		// first write the first 6 bits of the first byte
		output[out_idx++] = Blob::BASE64_MAP[(input_data[i] >> 2) & 0x3F];
		// now check the character count
		if (i == input_size - 1) {
			// single byte left over: convert the remainder of that byte and insert padding
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4)];
			output[out_idx++] = Blob::BASE64_PADDING;
		} else {
			// two bytes left over: convert the second byte as well
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i] & 0x3) << 4) | ((input_data[i + 1] & 0xF0) >> 4)];
			output[out_idx++] = Blob::BASE64_MAP[((input_data[i + 1] & 0xF) << 2)];
		}
		output[out_idx++] = Blob::BASE64_PADDING;
	}
}

static constexpr int BASE64_DECODING_TABLE[256] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
    -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
    45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};

idx_t Blob::FromBase64Size(string_t str) {
	auto input_data = str.GetDataUnsafe();
	auto input_size = str.GetSize();
	if (input_size % 4 != 0) {
		// valid base64 needs to always be cleanly divisible by 4
		throw ConversionException("Could not decode string \"%s\" as base64: length must be a multiple of 4",
		                          str.GetString());
	}
	if (input_size < 4) {
		// empty string
		return 0;
	}
	auto base_size = input_size / 4 * 3;
	// check for padding to figure out the length
	if (input_data[input_size - 2] == Blob::BASE64_PADDING) {
		// two bytes of padding
		return base_size - 2;
	}
	if (input_data[input_size - 1] == Blob::BASE64_PADDING) {
		// one byte of padding
		return base_size - 1;
	}
	// no padding
	return base_size;
}

template <bool ALLOW_PADDING>
uint32_t DecodeBase64Bytes(const string_t &str, const_data_ptr_t input_data, idx_t base_idx) {
	int decoded_bytes[4];
	for (idx_t decode_idx = 0; decode_idx < 4; decode_idx++) {
		if (ALLOW_PADDING && decode_idx >= 2 && input_data[base_idx + decode_idx] == Blob::BASE64_PADDING) {
			// the last two bytes of a base64 string can have padding: in this case we set the byte to 0
			decoded_bytes[decode_idx] = 0;
		} else {
			decoded_bytes[decode_idx] = BASE64_DECODING_TABLE[input_data[base_idx + decode_idx]];
		}
		if (decoded_bytes[decode_idx] < 0) {
			throw ConversionException(
			    "Could not decode string \"%s\" as base64: invalid byte value '%d' at position %d", str.GetString(),
			    input_data[base_idx + decode_idx], base_idx + decode_idx);
		}
	}
	return (decoded_bytes[0] << 3 * 6) + (decoded_bytes[1] << 2 * 6) + (decoded_bytes[2] << 1 * 6) +
	       (decoded_bytes[3] << 0 * 6);
}

void Blob::FromBase64(string_t str, data_ptr_t output, idx_t output_size) {
	D_ASSERT(output_size == FromBase64Size(str));
	auto input_data = (const_data_ptr_t)str.GetDataUnsafe();
	auto input_size = str.GetSize();
	if (input_size == 0) {
		return;
	}
	idx_t out_idx = 0;
	idx_t i = 0;
	for (i = 0; i + 4 < input_size; i += 4) {
		auto combined = DecodeBase64Bytes<false>(str, input_data, i);
		output[out_idx++] = (combined >> 2 * 8) & 0xFF;
		output[out_idx++] = (combined >> 1 * 8) & 0xFF;
		output[out_idx++] = (combined >> 0 * 8) & 0xFF;
	}
	// decode the final four bytes: padding is allowed here
	auto combined = DecodeBase64Bytes<true>(str, input_data, i);
	output[out_idx++] = (combined >> 2 * 8) & 0xFF;
	if (out_idx < output_size) {
		output[out_idx++] = (combined >> 1 * 8) & 0xFF;
	}
	if (out_idx < output_size) {
		output[out_idx++] = (combined >> 0 * 8) & 0xFF;
	}
}

} // namespace duckdb



namespace duckdb {

const int64_t NumericHelper::POWERS_OF_TEN[] {1,
                                              10,
                                              100,
                                              1000,
                                              10000,
                                              100000,
                                              1000000,
                                              10000000,
                                              100000000,
                                              1000000000,
                                              10000000000,
                                              100000000000,
                                              1000000000000,
                                              10000000000000,
                                              100000000000000,
                                              1000000000000000,
                                              10000000000000000,
                                              100000000000000000,
                                              1000000000000000000};

const double NumericHelper::DOUBLE_POWERS_OF_TEN[] {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
                                                    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
                                                    1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29,
                                                    1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38, 1e39};

template <>
int NumericHelper::UnsignedLength(uint8_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint16_t value) {
	int length = 1;
	length += value >= 10;
	length += value >= 100;
	length += value >= 1000;
	length += value >= 10000;
	return length;
}

template <>
int NumericHelper::UnsignedLength(uint32_t value) {
	if (value >= 10000) {
		int length = 5;
		length += value >= 100000;
		length += value >= 1000000;
		length += value >= 10000000;
		length += value >= 100000000;
		length += value >= 1000000000;
		return length;
	} else {
		int length = 1;
		length += value >= 10;
		length += value >= 100;
		length += value >= 1000;
		return length;
	}
}

template <>
int NumericHelper::UnsignedLength(uint64_t value) {
	if (value >= 10000000000ULL) {
		if (value >= 1000000000000000ULL) {
			int length = 16;
			length += value >= 10000000000000000ULL;
			length += value >= 100000000000000000ULL;
			length += value >= 1000000000000000000ULL;
			length += value >= 10000000000000000000ULL;
			return length;
		} else {
			int length = 11;
			length += value >= 100000000000ULL;
			length += value >= 1000000000000ULL;
			length += value >= 10000000000000ULL;
			length += value >= 100000000000000ULL;
			return length;
		}
	} else {
		if (value >= 100000ULL) {
			int length = 6;
			length += value >= 1000000ULL;
			length += value >= 10000000ULL;
			length += value >= 100000000ULL;
			length += value >= 1000000000ULL;
			return length;
		} else {
			int length = 1;
			length += value >= 10ULL;
			length += value >= 100ULL;
			length += value >= 1000ULL;
			length += value >= 10000ULL;
			return length;
		}
	}
}

template <>
std::string NumericHelper::ToString(hugeint_t value) {
	return Hugeint::ToString(value);
}

} // namespace duckdb










#include <algorithm>
#include <cstring>

namespace duckdb {

ChunkCollection::ChunkCollection(Allocator &allocator) : allocator(allocator), count(0) {
}

ChunkCollection::ChunkCollection(ClientContext &context) : ChunkCollection(Allocator::Get(context)) {
}

void ChunkCollection::Verify() {
#ifdef DEBUG
	for (auto &chunk : chunks) {
		chunk->Verify();
	}
#endif
}

void ChunkCollection::Append(ChunkCollection &other) {
	for (auto &chunk : other.chunks) {
		Append(*chunk);
	}
}

void ChunkCollection::Merge(ChunkCollection &other) {
	if (other.count == 0) {
		return;
	}
	if (count == 0) {
		chunks = move(other.chunks);
		types = move(other.types);
		count = other.count;
		return;
	}
	unique_ptr<DataChunk> old_back;
	if (!chunks.empty() && chunks.back()->size() != STANDARD_VECTOR_SIZE) {
		old_back = move(chunks.back());
		chunks.pop_back();
		count -= old_back->size();
	}
	for (auto &chunk : other.chunks) {
		chunks.push_back(move(chunk));
	}
	count += other.count;
	if (old_back) {
		Append(*old_back);
	}
	Verify();
}

void ChunkCollection::Append(DataChunk &new_chunk) {
	if (new_chunk.size() == 0) {
		return;
	}
	new_chunk.Verify();

	// we have to ensure that every chunk in the ChunkCollection is completely
	// filled, otherwise our O(1) lookup in GetValue and SetValue does not work
	// first fill the latest chunk, if it exists
	count += new_chunk.size();

	idx_t remaining_data = new_chunk.size();
	idx_t offset = 0;
	if (chunks.empty()) {
		// first chunk
		types = new_chunk.GetTypes();
	} else {
		// the types of the new chunk should match the types of the previous one
		D_ASSERT(types.size() == new_chunk.ColumnCount());
		auto new_types = new_chunk.GetTypes();
		for (idx_t i = 0; i < types.size(); i++) {
			if (new_types[i] != types[i]) {
				throw TypeMismatchException(new_types[i], types[i], "Type mismatch when combining rows");
			}
			if (types[i].InternalType() == PhysicalType::LIST) {
				// need to check all the chunks because they can have only-null list entries
				for (auto &chunk : chunks) {
					auto &chunk_vec = chunk->data[i];
					auto &new_vec = new_chunk.data[i];
					auto &chunk_type = chunk_vec.GetType();
					auto &new_type = new_vec.GetType();
					if (chunk_type != new_type) {
						throw TypeMismatchException(chunk_type, new_type, "Type mismatch when combining lists");
					}
				}
			}
			// TODO check structs, too
		}

		// first append data to the current chunk
		DataChunk &last_chunk = *chunks.back();
		idx_t added_data = MinValue<idx_t>(remaining_data, STANDARD_VECTOR_SIZE - last_chunk.size());
		if (added_data > 0) {
			// copy <added_data> elements to the last chunk
			new_chunk.Normalify();
			// have to be careful here: setting the cardinality without calling normalify can cause incorrect partial
			// decompression
			idx_t old_count = new_chunk.size();
			new_chunk.SetCardinality(added_data);

			last_chunk.Append(new_chunk);
			remaining_data -= added_data;
			// reset the chunk to the old data
			new_chunk.SetCardinality(old_count);
			offset = added_data;
		}
	}

	if (remaining_data > 0) {
		// create a new chunk and fill it with the remainder
		auto chunk = make_unique<DataChunk>();
		chunk->Initialize(allocator, types);
		new_chunk.Copy(*chunk, offset);
		chunks.push_back(move(chunk));
	}
}

void ChunkCollection::Append(unique_ptr<DataChunk> new_chunk) {
	if (types.empty()) {
		types = new_chunk->GetTypes();
	}
	D_ASSERT(types == new_chunk->GetTypes());
	count += new_chunk->size();
	chunks.push_back(move(new_chunk));
}

void ChunkCollection::Fuse(ChunkCollection &other) {
	if (count == 0) {
		chunks.reserve(other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < other.ChunkCount(); ++chunk_idx) {
			auto lhs = make_unique<DataChunk>();
			auto &rhs = other.GetChunk(chunk_idx);
			lhs->data.reserve(rhs.data.size());
			for (auto &v : rhs.data) {
				lhs->data.emplace_back(Vector(v));
			}
			lhs->SetCardinality(rhs.size());
			chunks.push_back(move(lhs));
		}
		count = other.Count();
	} else {
		D_ASSERT(this->ChunkCount() == other.ChunkCount());
		for (idx_t chunk_idx = 0; chunk_idx < ChunkCount(); ++chunk_idx) {
			auto &lhs = this->GetChunk(chunk_idx);
			auto &rhs = other.GetChunk(chunk_idx);
			D_ASSERT(lhs.size() == rhs.size());
			for (auto &v : rhs.data) {
				lhs.data.emplace_back(Vector(v));
			}
		}
	}
	types.insert(types.end(), other.types.begin(), other.types.end());
}

// returns an int similar to a C comparator:
// -1 if left < right
// 0 if left == right
// 1 if left > right

template <class TYPE>
static int8_t TemplatedCompareValue(Vector &left_vec, Vector &right_vec, idx_t left_idx, idx_t right_idx) {
	D_ASSERT(left_vec.GetType() == right_vec.GetType());
	auto left_val = FlatVector::GetData<TYPE>(left_vec)[left_idx];
	auto right_val = FlatVector::GetData<TYPE>(right_vec)[right_idx];
	if (Equals::Operation<TYPE>(left_val, right_val)) {
		return 0;
	}
	if (LessThan::Operation<TYPE>(left_val, right_val)) {
		return -1;
	}
	return 1;
}

// return type here is int32 because strcmp() on some platforms returns rather large values
static int32_t CompareValue(Vector &left_vec, Vector &right_vec, idx_t vector_idx_left, idx_t vector_idx_right,
                            OrderByNullType null_order) {
	auto left_null = FlatVector::IsNull(left_vec, vector_idx_left);
	auto right_null = FlatVector::IsNull(right_vec, vector_idx_right);

	if (left_null && right_null) {
		return 0;
	} else if (right_null) {
		return null_order == OrderByNullType::NULLS_FIRST ? 1 : -1;
	} else if (left_null) {
		return null_order == OrderByNullType::NULLS_FIRST ? -1 : 1;
	}

	switch (left_vec.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return TemplatedCompareValue<int8_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::INT16:
		return TemplatedCompareValue<int16_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::INT32:
		return TemplatedCompareValue<int32_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::INT64:
		return TemplatedCompareValue<int64_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::UINT8:
		return TemplatedCompareValue<uint8_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::UINT16:
		return TemplatedCompareValue<uint16_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::UINT32:
		return TemplatedCompareValue<uint32_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::UINT64:
		return TemplatedCompareValue<uint64_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::INT128:
		return TemplatedCompareValue<hugeint_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::FLOAT:
		return TemplatedCompareValue<float>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::DOUBLE:
		return TemplatedCompareValue<double>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::VARCHAR:
		return TemplatedCompareValue<string_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	case PhysicalType::INTERVAL:
		return TemplatedCompareValue<interval_t>(left_vec, right_vec, vector_idx_left, vector_idx_right);
	default:
		throw NotImplementedException("Type for comparison");
	}
}

static int CompareTuple(ChunkCollection *sort_by, vector<OrderType> &desc, vector<OrderByNullType> &null_order,
                        idx_t left, idx_t right) {
	D_ASSERT(sort_by);

	idx_t chunk_idx_left = left / STANDARD_VECTOR_SIZE;
	idx_t chunk_idx_right = right / STANDARD_VECTOR_SIZE;
	idx_t vector_idx_left = left % STANDARD_VECTOR_SIZE;
	idx_t vector_idx_right = right % STANDARD_VECTOR_SIZE;

	auto &left_chunk = sort_by->GetChunk(chunk_idx_left);
	auto &right_chunk = sort_by->GetChunk(chunk_idx_right);

	for (idx_t col_idx = 0; col_idx < desc.size(); col_idx++) {
		auto order_type = desc[col_idx];

		auto &left_vec = left_chunk.data[col_idx];
		auto &right_vec = right_chunk.data[col_idx];

		D_ASSERT(left_vec.GetVectorType() == VectorType::FLAT_VECTOR);
		D_ASSERT(right_vec.GetVectorType() == VectorType::FLAT_VECTOR);
		D_ASSERT(left_vec.GetType() == right_vec.GetType());

		auto comp_res = CompareValue(left_vec, right_vec, vector_idx_left, vector_idx_right, null_order[col_idx]);

		if (comp_res == 0) {
			continue;
		}
		return comp_res < 0 ? (order_type == OrderType::ASCENDING ? -1 : 1)
		                    : (order_type == OrderType::ASCENDING ? 1 : -1);
	}
	return 0;
}

static int64_t QuicksortInitial(ChunkCollection *sort_by, vector<OrderType> &desc, vector<OrderByNullType> &null_order,
                                idx_t *result) {
	// select pivot
	int64_t pivot = 0;
	int64_t low = 0, high = sort_by->Count() - 1;
	// now insert elements
	for (idx_t i = 1; i < sort_by->Count(); i++) {
		if (CompareTuple(sort_by, desc, null_order, i, pivot) <= 0) {
			result[low++] = i;
		} else {
			result[high--] = i;
		}
	}
	D_ASSERT(low == high);
	result[low] = pivot;
	return low;
}

struct QuicksortInfo {
	QuicksortInfo(int64_t left_p, int64_t right_p) : left(left_p), right(right_p) {
	}

	int64_t left;
	int64_t right;
};

struct QuicksortStack {
	std::queue<QuicksortInfo> info_queue;

	QuicksortInfo Pop() {
		auto element = info_queue.front();
		info_queue.pop();
		return element;
	}

	bool IsEmpty() {
		return info_queue.empty();
	}

	void Enqueue(int64_t left, int64_t right) {
		if (left >= right) {
			return;
		}
		info_queue.emplace(left, right);
	}
};

static void QuicksortInPlace(ChunkCollection *sort_by, vector<OrderType> &desc, vector<OrderByNullType> &null_order,
                             idx_t *result, QuicksortInfo info, QuicksortStack &stack) {
	auto left = info.left;
	auto right = info.right;

	D_ASSERT(left < right);

	int64_t middle = left + (right - left) / 2;
	int64_t pivot = result[middle];
	// move the mid point value to the front.
	int64_t i = left + 1;
	int64_t j = right;

	std::swap(result[middle], result[left]);
	bool all_equal = true;
	while (i <= j) {
		if (result) {
			while (i <= j) {
				int cmp = CompareTuple(sort_by, desc, null_order, result[i], pivot);
				if (cmp < 0) {
					all_equal = false;
				} else if (cmp > 0) {
					all_equal = false;
					break;
				}
				i++;
			}
		}

		while (i <= j && CompareTuple(sort_by, desc, null_order, result[j], pivot) > 0) {
			j--;
		}

		if (i < j) {
			std::swap(result[i], result[j]);
		}
	}
	std::swap(result[i - 1], result[left]);
	int64_t part = i - 1;

	if (all_equal) {
		return;
	}

	stack.Enqueue(left, part - 1);
	stack.Enqueue(part + 1, right);
}

void ChunkCollection::Sort(vector<OrderType> &desc, vector<OrderByNullType> &null_order, idx_t result[]) {
	if (count == 0) {
		return;
	}
	D_ASSERT(result);

	// start off with an initial quicksort
	int64_t part = QuicksortInitial(this, desc, null_order, result);

	// now continuously perform
	QuicksortStack stack;
	stack.Enqueue(0, part);
	stack.Enqueue(part + 1, count - 1);
	while (!stack.IsEmpty()) {
		auto element = stack.Pop();
		QuicksortInPlace(this, desc, null_order, result, element, stack);
	}
}

// FIXME make this more efficient by not using the Value API
// just use memcpy in the vectors
// assert that there is no selection list
void ChunkCollection::Reorder(idx_t order_org[]) {
	auto order = unique_ptr<idx_t[]>(new idx_t[count]);
	memcpy(order.get(), order_org, sizeof(idx_t) * count);

	// adapted from https://stackoverflow.com/a/7366196/2652376

	auto val_buf = vector<Value>();
	val_buf.resize(ColumnCount());

	idx_t j, k;
	for (idx_t i = 0; i < count; i++) {
		for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
			val_buf[col_idx] = GetValue(col_idx, i);
		}
		j = i;
		while (true) {
			k = order[j];
			order[j] = j;
			if (k == i) {
				break;
			}
			for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
				SetValue(col_idx, j, GetValue(col_idx, k));
			}
			j = k;
		}
		for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
			SetValue(col_idx, j, val_buf[col_idx]);
		}
	}
}

Value ChunkCollection::GetValue(idx_t column, idx_t index) {
	return chunks[LocateChunk(index)]->GetValue(column, index % STANDARD_VECTOR_SIZE);
}

void ChunkCollection::SetValue(idx_t column, idx_t index, const Value &value) {
	chunks[LocateChunk(index)]->SetValue(column, index % STANDARD_VECTOR_SIZE, value);
}

void ChunkCollection::CopyCell(idx_t column, idx_t index, Vector &target, idx_t target_offset) {
	auto &chunk = GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	VectorOperations::Copy(source, target, source_offset + 1, source_offset, target_offset);
}

string ChunkCollection::ToString() const {
	return chunks.empty() ? "ChunkCollection [ 0 ]"
	                      : "ChunkCollection [ " + std::to_string(count) + " ]: \n" + chunks[0]->ToString();
}

void ChunkCollection::Print() const {
	Printer::Print(ToString());
}

bool ChunkCollection::Equals(ChunkCollection &other) {
	if (count != other.count) {
		return false;
	}
	if (ColumnCount() != other.ColumnCount()) {
		return false;
	}
	// first try to compare the results as-is
	bool compare_equals = true;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
			auto lvalue = GetValue(col_idx, row_idx);
			auto rvalue = other.GetValue(col_idx, row_idx);
			if (!Value::ValuesAreEqual(lvalue, rvalue)) {
				compare_equals = false;
				break;
			}
		}
		if (!compare_equals) {
			break;
		}
	}
	if (compare_equals) {
		return true;
	}
	for (auto &type : types) {
		// sort not supported
		if (type.InternalType() == PhysicalType::LIST || type.InternalType() == PhysicalType::STRUCT) {
			return false;
		}
	}
	// if the results are not equal,
	// sort both chunk collections to ensure the comparison is not order insensitive
	vector<OrderType> desc(ColumnCount(), OrderType::DESCENDING);
	vector<OrderByNullType> null_order(ColumnCount(), OrderByNullType::NULLS_FIRST);
	auto this_order = unique_ptr<idx_t[]>(new idx_t[count]);
	auto other_order = unique_ptr<idx_t[]>(new idx_t[count]);
	Sort(desc, null_order, this_order.get());
	other.Sort(desc, null_order, other_order.get());

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto lrow = this_order[row_idx];
		auto rrow = other_order[row_idx];
		for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
			auto lvalue = GetValue(col_idx, lrow);
			auto rvalue = other.GetValue(col_idx, rrow);
			if (!Value::ValuesAreEqual(lvalue, rvalue)) {
				return false;
			}
		}
	}
	return true;
}

} // namespace duckdb






















namespace duckdb {

DataChunk::DataChunk() : count(0), capacity(STANDARD_VECTOR_SIZE) {
}

DataChunk::~DataChunk() {
}

void DataChunk::InitializeEmpty(const vector<LogicalType> &types) {
	capacity = STANDARD_VECTOR_SIZE;
	D_ASSERT(data.empty());   // can only be initialized once
	D_ASSERT(!types.empty()); // empty chunk not allowed
	for (idx_t i = 0; i < types.size(); i++) {
		data.emplace_back(Vector(types[i], nullptr));
	}
}

void DataChunk::Initialize(Allocator &allocator, const vector<LogicalType> &types) {
	D_ASSERT(data.empty());   // can only be initialized once
	D_ASSERT(!types.empty()); // empty chunk not allowed
	capacity = STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < types.size(); i++) {
		VectorCache cache(allocator, types[i]);
		data.emplace_back(cache);
		vector_caches.push_back(move(cache));
	}
}

void DataChunk::Initialize(ClientContext &context, const vector<LogicalType> &types) {
	Initialize(Allocator::Get(context), types);
}

void DataChunk::Reset() {
	if (data.empty()) {
		return;
	}
	if (vector_caches.size() != data.size()) {
		throw InternalException("VectorCache and column count mismatch in DataChunk::Reset");
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].ResetFromCache(vector_caches[i]);
	}
	capacity = STANDARD_VECTOR_SIZE;
	SetCardinality(0);
}

void DataChunk::Destroy() {
	data.clear();
	vector_caches.clear();
	capacity = 0;
	SetCardinality(0);
}

Value DataChunk::GetValue(idx_t col_idx, idx_t index) const {
	D_ASSERT(index < size());
	return data[col_idx].GetValue(index);
}

void DataChunk::SetValue(idx_t col_idx, idx_t index, const Value &val) {
	data[col_idx].SetValue(index, val);
}

void DataChunk::Reference(DataChunk &chunk) {
	D_ASSERT(chunk.ColumnCount() <= ColumnCount());
	SetCardinality(chunk);
	SetCapacity(chunk);
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		data[i].Reference(chunk.data[i]);
	}
}

void DataChunk::Move(DataChunk &chunk) {
	SetCardinality(chunk);
	SetCapacity(chunk);
	data = move(chunk.data);
	vector_caches = move(chunk.vector_caches);

	chunk.Destroy();
}

void DataChunk::Copy(DataChunk &other, idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], size(), offset, 0);
	}
	other.SetCardinality(size() - offset);
}

void DataChunk::Copy(DataChunk &other, const SelectionVector &sel, const idx_t source_count, const idx_t offset) const {
	D_ASSERT(ColumnCount() == other.ColumnCount());
	D_ASSERT(other.size() == 0);
	D_ASSERT((offset + source_count) <= size());

	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(other.data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		VectorOperations::Copy(data[i], other.data[i], sel, source_count, offset, 0);
	}
	other.SetCardinality(source_count - offset);
}

void DataChunk::Split(DataChunk &other, idx_t split_idx) {
	D_ASSERT(other.size() == 0);
	D_ASSERT(other.data.empty());
	D_ASSERT(split_idx < data.size());
	const idx_t num_cols = data.size();
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		other.data.push_back(move(data[col_idx]));
		other.vector_caches.push_back(move(vector_caches[col_idx]));
	}
	for (idx_t col_idx = split_idx; col_idx < num_cols; col_idx++) {
		data.pop_back();
		vector_caches.pop_back();
	}
	other.SetCardinality(*this);
	other.SetCapacity(*this);
}

void DataChunk::Fuse(DataChunk &other) {
	D_ASSERT(other.size() == size());
	const idx_t num_cols = other.data.size();
	for (idx_t col_idx = 0; col_idx < num_cols; ++col_idx) {
		data.emplace_back(move(other.data[col_idx]));
		vector_caches.emplace_back(move(other.vector_caches[col_idx]));
	}
	other.Destroy();
}

void DataChunk::Append(const DataChunk &other, bool resize, SelectionVector *sel, idx_t sel_count) {
	idx_t new_size = sel ? size() + sel_count : size() + other.size();
	if (other.size() == 0) {
		return;
	}
	if (ColumnCount() != other.ColumnCount()) {
		throw InternalException("Column counts of appending chunk doesn't match!");
	}
	if (new_size > capacity) {
		if (resize) {
			for (idx_t i = 0; i < ColumnCount(); i++) {
				data[i].Resize(size(), new_size);
			}
			capacity = new_size;
		} else {
			throw InternalException("Can't append chunk to other chunk without resizing");
		}
	}
	for (idx_t i = 0; i < ColumnCount(); i++) {
		D_ASSERT(data[i].GetVectorType() == VectorType::FLAT_VECTOR);
		if (sel) {
			VectorOperations::Copy(other.data[i], data[i], *sel, sel_count, 0, size());
		} else {
			VectorOperations::Copy(other.data[i], data[i], other.size(), 0, size());
		}
	}
	SetCardinality(new_size);
}

void DataChunk::Normalify() {
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Normalify(size());
	}
}

vector<LogicalType> DataChunk::GetTypes() {
	vector<LogicalType> types;
	for (idx_t i = 0; i < ColumnCount(); i++) {
		types.push_back(data[i].GetType());
	}
	return types;
}

string DataChunk::ToString() const {
	string retval = "Chunk - [" + to_string(ColumnCount()) + " Columns]\n";
	for (idx_t i = 0; i < ColumnCount(); i++) {
		retval += "- " + data[i].ToString(size()) + "\n";
	}
	return retval;
}

void DataChunk::Serialize(Serializer &serializer) {
	// write the count
	serializer.Write<sel_t>(size());
	serializer.Write<idx_t>(ColumnCount());
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		// write the types
		data[col_idx].GetType().Serialize(serializer);
	}
	// write the data
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].Serialize(size(), serializer);
	}
}

void DataChunk::Deserialize(Deserializer &source) {
	auto rows = source.Read<sel_t>();
	idx_t column_count = source.Read<idx_t>();

	vector<LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		types.push_back(LogicalType::Deserialize(source));
	}
	Initialize(Allocator::DefaultAllocator(), types);
	// now load the column data
	SetCardinality(rows);
	for (idx_t i = 0; i < column_count; i++) {
		data[i].Deserialize(rows, source);
	}
	Verify();
}

void DataChunk::Slice(const SelectionVector &sel_vector, idx_t count_p) {
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < ColumnCount(); c++) {
		data[c].Slice(sel_vector, count_p, merge_cache);
	}
}

void DataChunk::Slice(DataChunk &other, const SelectionVector &sel, idx_t count_p, idx_t col_offset) {
	D_ASSERT(other.ColumnCount() <= col_offset + ColumnCount());
	this->count = count_p;
	SelCache merge_cache;
	for (idx_t c = 0; c < other.ColumnCount(); c++) {
		if (other.data[c].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
			// already a dictionary! merge the dictionaries
			data[col_offset + c].Reference(other.data[c]);
			data[col_offset + c].Slice(sel, count_p, merge_cache);
		} else {
			data[col_offset + c].Slice(other.data[c], sel, count_p);
		}
	}
}

unique_ptr<VectorData[]> DataChunk::Orrify() {
	auto orrified_data = unique_ptr<VectorData[]>(new VectorData[ColumnCount()]);
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		data[col_idx].Orrify(size(), orrified_data[col_idx]);
	}
	return orrified_data;
}

void DataChunk::Hash(Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalType::HASH);
	VectorOperations::Hash(data[0], result, size());
	for (idx_t i = 1; i < ColumnCount(); i++) {
		VectorOperations::CombineHash(result, data[i], size());
	}
}

void DataChunk::Verify() {
#ifdef DEBUG
	D_ASSERT(size() <= capacity);
	// verify that all vectors in this chunk have the chunk selection vector
	for (idx_t i = 0; i < ColumnCount(); i++) {
		data[i].Verify(size());
	}
#endif
}

void DataChunk::Print() {
	Printer::Print(ToString());
}

struct DuckDBArrowArrayChildHolder {
	ArrowArray array;
	//! need max three pointers for strings
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	unique_ptr<Vector> vector;
	unique_ptr<data_t[]> offsets;
	unique_ptr<data_t[]> data;
	//! Children of nested structures
	::duckdb::vector<DuckDBArrowArrayChildHolder> children;
	::duckdb::vector<ArrowArray *> children_ptrs;
};

struct DuckDBArrowArrayHolder {
	vector<DuckDBArrowArrayChildHolder> children = {};
	vector<ArrowArray *> children_ptrs = {};
	array<const void *, 1> buffers = {{nullptr}};
	vector<shared_ptr<ArrowArrayWrapper>> arrow_original_array;
};

static void ReleaseDuckDBArrowArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = static_cast<DuckDBArrowArrayHolder *>(array->private_data);
	delete holder;
}

void InitializeChild(DuckDBArrowArrayChildHolder &child_holder, idx_t size) {
	auto &child = child_holder.array;
	child.private_data = nullptr;
	child.release = ReleaseDuckDBArrowArray;
	child.n_children = 0;
	child.null_count = 0;
	child.offset = 0;
	child.dictionary = nullptr;
	child.buffers = child_holder.buffers.data();

	child.length = size;
}

void SetChildValidityMask(Vector &vector, ArrowArray &child) {
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
	auto &mask = FlatVector::Validity(vector);
	if (!mask.AllValid()) {
		//! any bits are set: might have nulls
		child.null_count = -1;
	} else {
		//! no bits are set; we know there are no nulls
		child.null_count = 0;
	}
	child.buffers[0] = (void *)mask.GetData();
}

void SetArrowChild(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size);

void SetList(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
	auto &child = child_holder.array;
	child_holder.vector = make_unique<Vector>(data);

	//! Lists have two buffers
	child.n_buffers = 2;
	//! Second Buffer is the list offsets
	child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
	child.buffers[1] = child_holder.offsets.get();
	auto offset_ptr = (uint32_t *)child.buffers[1];
	auto list_data = FlatVector::GetData<list_entry_t>(data);
	auto list_mask = FlatVector::Validity(data);
	idx_t offset = 0;
	offset_ptr[0] = 0;
	for (idx_t i = 0; i < size; i++) {
		auto &le = list_data[i];

		if (list_mask.RowIsValid(i)) {
			offset += le.length;
		}

		offset_ptr[i + 1] = offset;
	}
	auto list_size = ListVector::GetListSize(data);
	child_holder.children.resize(1);
	InitializeChild(child_holder.children[0], list_size);
	child.n_children = 1;
	child_holder.children_ptrs.push_back(&child_holder.children[0].array);
	child.children = &child_holder.children_ptrs[0];
	auto &child_vector = ListVector::GetEntry(data);
	auto &child_type = ListType::GetChildType(type);
	SetArrowChild(child_holder.children[0], child_type, child_vector, list_size);
	SetChildValidityMask(child_vector, child_holder.children[0].array);
}

void SetStruct(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
	auto &child = child_holder.array;
	child_holder.vector = make_unique<Vector>(data);

	//! Structs only have validity buffers
	child.n_buffers = 1;
	auto &children = StructVector::GetEntries(*child_holder.vector);
	child.n_children = children.size();
	child_holder.children.resize(child.n_children);
	for (auto &struct_child : child_holder.children) {
		InitializeChild(struct_child, size);
		child_holder.children_ptrs.push_back(&struct_child.array);
	}
	child.children = &child_holder.children_ptrs[0];
	for (idx_t child_idx = 0; child_idx < child_holder.children.size(); child_idx++) {
		SetArrowChild(child_holder.children[child_idx], StructType::GetChildType(type, child_idx), *children[child_idx],
		              size);
		SetChildValidityMask(*children[child_idx], child_holder.children[child_idx].array);
	}
}

void SetStructMap(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
	auto &child = child_holder.array;
	child_holder.vector = make_unique<Vector>(data);

	//! Structs only have validity buffers
	child.n_buffers = 1;
	auto &children = StructVector::GetEntries(*child_holder.vector);
	child.n_children = children.size();
	child_holder.children.resize(child.n_children);
	auto list_size = ListVector::GetListSize(*children[0]);
	child.length = list_size;
	for (auto &struct_child : child_holder.children) {
		InitializeChild(struct_child, list_size);
		child_holder.children_ptrs.push_back(&struct_child.array);
	}
	child.children = &child_holder.children_ptrs[0];
	auto &child_types = StructType::GetChildTypes(type);
	for (idx_t child_idx = 0; child_idx < child_holder.children.size(); child_idx++) {
		auto &list_vector_child = ListVector::GetEntry(*children[child_idx]);
		if (child_idx == 0) {
			VectorData list_data;
			children[child_idx]->Orrify(size, list_data);
			auto list_child_validity = FlatVector::Validity(list_vector_child);
			if (!list_child_validity.AllValid()) {
				//! Get the offsets to check from the selection vector
				auto list_offsets = FlatVector::GetData<list_entry_t>(*children[child_idx]);
				for (idx_t list_idx = 0; list_idx < size; list_idx++) {
					auto offset = list_offsets[list_data.sel->get_index(list_idx)];
					if (!list_child_validity.CheckAllValid(offset.length + offset.offset, offset.offset)) {
						throw std::runtime_error("Arrow doesnt accept NULL keys on Maps");
					}
				}
			}
		} else {
			SetChildValidityMask(list_vector_child, child_holder.children[child_idx].array);
		}
		SetArrowChild(child_holder.children[child_idx], ListType::GetChildType(child_types[child_idx].second),
		              list_vector_child, list_size);
	}
}

struct ArrowUUIDConversion {
	using internal_type_t = hugeint_t;

	static unique_ptr<Vector> InitializeVector(Vector &data, idx_t size) {
		return make_unique<Vector>(LogicalType::VARCHAR, size);
	}

	static idx_t GetStringLength(hugeint_t value) {
		return UUID::STRING_SIZE;
	}

	static string_t ConvertValue(Vector &tgt_vec, string_t *tgt_ptr, internal_type_t *src_ptr, idx_t row) {
		auto str_value = UUID::ToString(src_ptr[row]);
		// Have to store this string
		tgt_ptr[row] = StringVector::AddStringOrBlob(tgt_vec, str_value);
		return tgt_ptr[row];
	}
};

struct ArrowVarcharConversion {
	using internal_type_t = string_t;

	static unique_ptr<Vector> InitializeVector(Vector &data, idx_t size) {
		return make_unique<Vector>(data);
	}
	static idx_t GetStringLength(string_t value) {
		return value.GetSize();
	}

	static string_t ConvertValue(Vector &tgt_vec, string_t *tgt_ptr, internal_type_t *src_ptr, idx_t row) {
		return src_ptr[row];
	}
};

template <class CONVERT, class VECTOR_TYPE>
void SetVarchar(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
	auto &child = child_holder.array;
	child_holder.vector = CONVERT::InitializeVector(data, size);
	auto target_data_ptr = FlatVector::GetData<string_t>(data);
	child.n_buffers = 3;
	child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
	child.buffers[1] = child_holder.offsets.get();
	D_ASSERT(child.buffers[1]);
	//! step 1: figure out total string length:
	idx_t total_string_length = 0;
	auto source_ptr = FlatVector::GetData<VECTOR_TYPE>(data);
	auto &mask = FlatVector::Validity(data);
	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		if (!mask.RowIsValid(row_idx)) {
			continue;
		}
		total_string_length += CONVERT::GetStringLength(source_ptr[row_idx]);
	}
	//! step 2: allocate this much
	child_holder.data = unique_ptr<data_t[]>(new data_t[total_string_length]);
	child.buffers[2] = child_holder.data.get();
	D_ASSERT(child.buffers[2]);
	//! step 3: assign buffers
	idx_t current_heap_offset = 0;
	auto target_ptr = (uint32_t *)child.buffers[1];

	for (idx_t row_idx = 0; row_idx < size; row_idx++) {
		target_ptr[row_idx] = current_heap_offset;
		if (!mask.RowIsValid(row_idx)) {
			continue;
		}
		string_t str = CONVERT::ConvertValue(*child_holder.vector, target_data_ptr, source_ptr, row_idx);
		memcpy((void *)((uint8_t *)child.buffers[2] + current_heap_offset), str.GetDataUnsafe(), str.GetSize());
		current_heap_offset += str.GetSize();
	}
	target_ptr[size] = current_heap_offset; //! need to terminate last string!
}

void SetArrowChild(DuckDBArrowArrayChildHolder &child_holder, const LogicalType &type, Vector &data, idx_t size) {
	auto &child = child_holder.array;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		//! Gotta bitpack these booleans
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		idx_t num_bytes = (size + 8 - 1) / 8;
		child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(uint8_t) * num_bytes]);
		child.buffers[1] = child_holder.data.get();
		auto source_ptr = FlatVector::GetData<uint8_t>(*child_holder.vector);
		auto target_ptr = (uint8_t *)child.buffers[1];
		idx_t target_pos = 0;
		idx_t cur_bit = 0;
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			if (cur_bit == 8) {
				target_pos++;
				cur_bit = 0;
			}
			if (source_ptr[row_idx] == 0) {
				//! We set the bit to 0
				target_ptr[target_pos] &= ~(1 << cur_bit);
			} else {
				//! We set the bit to 1
				target_ptr[target_pos] |= 1 << cur_bit;
			}
			cur_bit++;
		}
		break;
	}
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIME_TZ:
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child.buffers[1] = (void *)FlatVector::GetData(*child_holder.vector);
		break;
	case LogicalTypeId::SQLNULL:
		child.n_buffers = 1;
		break;
	case LogicalTypeId::DECIMAL: {
		child.n_buffers = 2;
		child_holder.vector = make_unique<Vector>(data);

		//! We have to convert to INT128
		switch (type.InternalType()) {

		case PhysicalType::INT16: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int16_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT32: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int32_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT64: {
			child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(hugeint_t) * (size)]);
			child.buffers[1] = child_holder.data.get();
			auto source_ptr = FlatVector::GetData<int64_t>(*child_holder.vector);
			auto target_ptr = (hugeint_t *)child.buffers[1];
			for (idx_t row_idx = 0; row_idx < size; row_idx++) {
				target_ptr[row_idx] = source_ptr[row_idx];
			}
			break;
		}
		case PhysicalType::INT128: {
			child.buffers[1] = (void *)FlatVector::GetData(*child_holder.vector);
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" + TypeIdToString(type.InternalType()));
		}
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		SetVarchar<ArrowVarcharConversion, string_t>(child_holder, type, data, size);
		break;
	}
	case LogicalTypeId::UUID: {
		SetVarchar<ArrowUUIDConversion, hugeint_t>(child_holder, type, data, size);
		break;
	}
	case LogicalTypeId::LIST: {
		SetList(child_holder, type, data, size);
		break;
	}
	case LogicalTypeId::STRUCT: {
		SetStruct(child_holder, type, data, size);
		break;
	}
	case LogicalTypeId::MAP: {
		child_holder.vector = make_unique<Vector>(data);

		auto &map_mask = FlatVector::Validity(*child_holder.vector);
		child.n_buffers = 2;
		//! Maps have one child
		child.n_children = 1;
		child_holder.children.resize(1);
		InitializeChild(child_holder.children[0], size);
		child_holder.children_ptrs.push_back(&child_holder.children[0].array);
		//! Second Buffer is the offsets
		child_holder.offsets = unique_ptr<data_t[]>(new data_t[sizeof(uint32_t) * (size + 1)]);
		child.buffers[1] = child_holder.offsets.get();
		auto &struct_children = StructVector::GetEntries(data);
		auto offset_ptr = (uint32_t *)child.buffers[1];
		auto list_data = FlatVector::GetData<list_entry_t>(*struct_children[0]);
		idx_t offset = 0;
		offset_ptr[0] = 0;
		for (idx_t i = 0; i < size; i++) {
			auto &le = list_data[i];
			if (map_mask.RowIsValid(i)) {
				offset += le.length;
			}
			offset_ptr[i + 1] = offset;
		}
		child.children = &child_holder.children_ptrs[0];
		//! We need to set up a struct
		auto struct_type = LogicalType::STRUCT(StructType::GetChildTypes(type));

		SetStructMap(child_holder.children[0], struct_type, *child_holder.vector, size);
		break;
	}
	case LogicalTypeId::INTERVAL: {
		//! convert interval from month/days/ucs to milliseconds
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child_holder.data = unique_ptr<data_t[]>(new data_t[sizeof(int64_t) * (size)]);
		child.buffers[1] = child_holder.data.get();
		auto source_ptr = FlatVector::GetData<interval_t>(*child_holder.vector);
		auto target_ptr = (int64_t *)child.buffers[1];
		for (idx_t row_idx = 0; row_idx < size; row_idx++) {
			target_ptr[row_idx] = Interval::GetMilli(source_ptr[row_idx]);
		}
		break;
	}
	case LogicalTypeId::ENUM: {
		// We need to initialize our dictionary
		child_holder.children.resize(1);
		idx_t dict_size = EnumType::GetSize(type);
		InitializeChild(child_holder.children[0], dict_size);
		Vector dictionary(EnumType::GetValuesInsertOrder(type));
		SetArrowChild(child_holder.children[0], dictionary.GetType(), dictionary, dict_size);
		child_holder.children_ptrs.push_back(&child_holder.children[0].array);

		// now we set the data
		child.dictionary = child_holder.children_ptrs[0];
		child_holder.vector = make_unique<Vector>(data);
		child.n_buffers = 2;
		child.buffers[1] = (void *)FlatVector::GetData(*child_holder.vector);

		break;
	}
	default:
		throw std::runtime_error("Unsupported type " + type.ToString());
	}
}

void DataChunk::ToArrowArray(ArrowArray *out_array) {
	Normalify();
	D_ASSERT(out_array);

	// Allocate as unique_ptr first to cleanup properly on error
	auto root_holder = make_unique<DuckDBArrowArrayHolder>();

	// Allocate the children
	root_holder->children.resize(ColumnCount());
	root_holder->children_ptrs.resize(ColumnCount(), nullptr);
	for (size_t i = 0; i < ColumnCount(); ++i) {
		root_holder->children_ptrs[i] = &root_holder->children[i].array;
	}
	out_array->children = root_holder->children_ptrs.data();
	out_array->n_children = ColumnCount();

	// Configure root array
	out_array->length = size();
	out_array->n_children = ColumnCount();
	out_array->n_buffers = 1;
	out_array->buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	out_array->offset = 0;
	out_array->null_count = 0; // needs to be 0
	out_array->dictionary = nullptr;

	//! Configure child arrays
	for (idx_t col_idx = 0; col_idx < ColumnCount(); col_idx++) {
		auto &child_holder = root_holder->children[col_idx];
		InitializeChild(child_holder, size());
		auto &vector = child_holder.vector;
		auto &child = child_holder.array;
		auto vec_buffer = data[col_idx].GetBuffer();
		if (vec_buffer->GetAuxiliaryData() &&
		    vec_buffer->GetAuxiliaryDataType() == VectorAuxiliaryDataType::ARROW_AUXILIARY) {
			auto arrow_aux_data = (ArrowAuxiliaryData *)vec_buffer->GetAuxiliaryData();
			root_holder->arrow_original_array.push_back(arrow_aux_data->arrow_array);
		}
		//! We could, in theory, output other types of vectors here, currently only FLAT Vectors
		SetArrowChild(child_holder, GetTypes()[col_idx], data[col_idx], size());
		SetChildValidityMask(*vector, child);
		out_array->children[col_idx] = &child;
	}

	// Release ownership to caller
	out_array->private_data = root_holder.release();
	out_array->release = ReleaseDuckDBArrowArray;
}

} // namespace duckdb
