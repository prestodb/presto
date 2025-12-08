==========
Procedures
==========

PrestoDB's procedures allow users to perform data manipulation and management tasks. Unlike traditional databases where procedural objects are
defined by users by using SQL, the procedures in PrestoDB are a set of system routines provided by developers through Connectors. The overall type hierarchy
is illustrated in the diagram below:

.. code-block:: text

    «abstract» BaseProcedure
        |-- Procedure // (Normal) Procedure
        |-- «abstract» DistributedProcedure
            |-- TableDataRewriteDistributedProcedure
            |-- ...... // Other future subtypes

PrestoDB supports two categories of procedures:

* **Normal Procedure**

These procedures are executed directly on the Coordinator node, and PrestoDB does not build distributed execution plans for them.
They are designed mainly for administrative tasks involving table or system metadata and cache management, such as ``sync_partition_metadata``
and ``invalidate_directory_list_cache`` in Hive Connector, or ``expire_snapshots`` and ``invalidate_statistics_file_cache`` in Iceberg Connector.

* **Distributed Procedure**

Procedures of this type are executed with a distributed execution plan constructed by PrestoDB, which utilizes the entire cluster of Worker nodes
for distributed computation. They are suitable for operations involving table data—such as data optimization, re-partitioning, sorting,
and pre-processing—as well as for administrative tasks that need to be executed across the Worker nodes, for instance, clearing caches on specific workers.

The type hierarchy for Distributed Procedures is designed to be extensible. Different distributed tasks can have different invocation parameters and
are planned into differently shaped execution plans; as such, they can be implemented as distinct subtypes of ``DistributedProcedure``.

For example, for table data rewrite tasks, PrestoDB provides the ``TableDataRewriteDistributedProcedure`` subtype.
Connector developers can leverage this subtype to implement specific data-rewrite distributed procedures—such as table data optimization, compression,
repartitioning, or sorting—for their connectors. Within the PrestoDB engine, tasks of this subtype are uniformly planned into an execution plan tree with the
following shape:

.. code-block:: text

    TableScanNode → FilterNode → CallDistributedProcedureNode → TableFinishNode → OutputNode

In addition, developers can implement other kinds of distributed procedures by extending the type hierarchy—defining new subtypes that are mapped to
execution plans of varying shapes.

For further design details, see `RFC-0021 for Presto <https://github.com/prestodb/rfcs/blob/main/RFC-0021-support-distributed-procedure.md>`_.

Normal Procedure
----------------

To make a procedure callable, a connector must first expose it to the PrestoDB engine. PrestoDB leverages the `Guice dependency injection framework <https://github.com/google/guice>`_
to manage procedure registration and lifecycle. A specific procedure is implemented and bound as a ``Provider<Procedure>``,
thus creating an instance only when it is actually needed for execution, enabling on-demand instantiation. The following steps will guide you on how to
implement and provide a procedure in a connector.

1. Procedure Provider Class
^^^^^^^^^^^^^^^^^^^^^^^^^^^

An implementation class must implement the ``Provider<Procedure>`` interface. Its constructor can use ``@Inject`` to receive any required dependencies
that are managed by Guice. The class must then implement the ``get()`` method from the Provider interface, which is responsible for constructing and
returning a new Procedure instance.

2. Creation of a Procedure Instance
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A Procedure object requires the following parameters upon creation:

* ``String schema`` - The schema namespace to which this procedure belongs (typically ``system`` in PrestoDB).
* ``String name`` - The name of this procedure, for example, ``expire_snapshots``.
* ``List<Argument> arguments`` - The parameter declarations list for this procedure.
* ``MethodHandle methodHandle`` - PrestoDB abstracts procedure execution through ``MethodHandle``. A procedure provider implements the core logic in
  a dedicated method and exposes it as a ``MethodHandle`` that is injected into the procedure instance.

.. note::

   * The Java method corresponding to the ``MethodHandle`` have a correspondence with the procedure parameters.

     * Its first parameter must be a session object of type ``ConnectorSession.class``.
     * The subsequent parameters must have a strict one-to-one correspondence in both type and order with the
       procedure parameters declared in the arguments list.

   * The method implementation for the ``MethodHandle`` must account for classloader isolation. Since PrestoDB employs a plugin isolation mechanism where each
     connector has its own ClassLoader, the engine must temporarily switch to the connector's specific ClassLoader when invoking its procedure logic. This context
     switch is critical to prevent ``ClassNotFoundException`` or ``NoClassDefFoundError`` issues.

As an example, the following is the ``expire_snapshots`` procedure implemented in the Iceberg connector:

.. code-block:: java

   public class ExpireSnapshotsProcedure
           implements Provider<Procedure>
   {
       private static final MethodHandle EXPIRE_SNAPSHOTS = methodHandle(
               ExpireSnapshotsProcedure.class,
               "expireSnapshots",
               ConnectorSession.class,
               String.class,
               String.class,
               SqlTimestamp.class,
               Integer.class,
               List.class);
       private final IcebergMetadataFactory metadataFactory;

       @Inject
       public ExpireSnapshotsProcedure(IcebergMetadataFactory metadataFactory)
       {
           this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
       }

       @Override
       public Procedure get()
       {
           return new Procedure(
                   "system",
                   "expire_snapshots",
                   ImmutableList.of(
                           new Argument("schema", VARCHAR),
                           new Argument("table_name", VARCHAR),
                           new Argument("older_than", TIMESTAMP, false, null),
                           new Argument("retain_last", INTEGER, false, null),
                           new Argument("snapshot_ids", "array(bigint)", false, null)),
                   EXPIRE_SNAPSHOTS.bindTo(this));
       }

       public void expireSnapshots(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan, Integer retainLast, List<Long> snapshotIds)
       {
           try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
               doExpireSnapshots(clientSession, schema, tableName, olderThan, retainLast, snapshotIds);
           }
       }

       private void doExpireSnapshots(ConnectorSession clientSession, String schema, String tableName, SqlTimestamp olderThan, Integer retainLast, List<Long> snapshotIds)
       {
           // Execute the snapshot expiration for the target table using the Iceberg interface
           // ......
       }
   }

3. Exposing the Procedure Provider to PrestoDB
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the Guice binding module of your target connector, add a binding for the procedure provider class defined above:

.. code-block:: java

    ......
    Multibinder<BaseProcedure<?>> procedures = newSetBinder(binder, new TypeLiteral<BaseProcedure<?>>() {});
    procedures.addBinding().toProvider(ExpireSnapshotsProcedure.class).in(Scopes.SINGLETON);
    ......

During startup, the PrestoDB engine collects the procedure providers exposed by all connectors and maintains them within their
respective namespaces (for example, ``hive.system`` or ``iceberg.system``). Once startup is complete, users can invoke these procedures by specifying
the corresponding connector namespace, for example:

.. code-block:: java

    call iceberg.system.expire_snapshots('default', 'test_table');
    call hive.system.invalidate_directory_list_cache();
    ......

Distributed Procedure
---------------------

PrestoDB supports building distributed execution plans for certain types of procedures, enabling them to leverage the calculation resources of
the entire cluster. Since different kinds of distributed procedures may correspond to distinct execution plan shapes, extending and implementing them
should be approached at two levels:

* For a category of procedures that share the same execution plan shape, extend a subtype of ``DistributedProcedure``. The currently supported
  ``TableDataRewriteDistributedProcedure`` subtype is designed for table data rewrite operations.
* Implement a concrete distributed procedure in a connector by building upon a specific ``DistributedProcedure`` subtype. For instance,
  ``rewrite_data_files`` in the Iceberg connector is built upon the ``TableDataRewriteDistributedProcedure`` subtype.

.. important::
    The ``DistributedProcedure`` class is abstract. Connector developers cannot implement it directly.
    You must build your concrete distributed procedure upon a specific **subtype** (like ``TableDataRewriteDistributedProcedure``)
    that the PrestoDB engine already knows how to analyze and plan.

Extending a DistributedProcedure Subtype
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Adding a DistributedProcedureType Enum Value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a new value to the ``DistributedProcedure.DistributedProcedureType`` enum, for example: ``TABLE_DATA_REWRITE``.

This enum value is important, as it is used during both the analysis and planning phases to distinguish between different ``DistributedProcedure`` subtypes
and execute the corresponding branch logic.

2. Creating a subclass of DistributedProcedure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new subclass of ``DistributedProcedure``, such as:

.. code-block:: java

   public class TableDataRewriteDistributedProcedure
           extends DistributedProcedure

* In the constructor, pass the corresponding ``DistributedProcedureType`` enum value such as ``TABLE_DATA_REWRITE`` to the ``super(...)`` method.
* In addition to the base parameters required by ``BaseProcedure`` (schema, name, and arguments, which are consistent with those in ``Procedure``),
  a subtype can define and is responsible for processing and validating any additional parameters it requires.

Additionally, the following three abstract methods defined by the base class ``DistributedProcedure`` should be implemented:

.. code-block:: java

   /**
    * This method creates a connector-specific, or even a distributed procedure subtype-specific, context object.
    * In connectors that support distributed procedures, it is invoked at the start of a distributed procedure's execution.
    * The generated procedure context is then bound to the current ConnectorMetadata to maintain all contextual information
    * involved throughout the execution, which may be utilized when the procedure finishes.
    */
   public ConnectorProcedureContext createContext()

   /**
    * Performs the preparatory work required when starting the execution of this distributed procedure
    * */
   public abstract ConnectorDistributedProcedureHandle begin(ConnectorSession session,
                                                           ConnectorProcedureContext procedureContext,
                                                           ConnectorTableLayoutHandle tableLayoutHandle,
                                                           Object[] arguments);

   /**
    * Performs the work required for the final centralized commit, after all distributed execution tasks have completed
    * */
   public abstract void finish(ConnectorProcedureContext procedureContext,
                             ConnectorDistributedProcedureHandle procedureHandle,
                             Collection<Slice> fragments);

.. note::

   At this architectural level, distributed procedure subtypes are designed to be decoupled from specific connectors. When implementing
   the three aforementioned abstract methods, it is recommended to focus solely on the common logic of the subtype. Connector-specific functionality
   should be abstracted into method interfaces and delegated to the final, concrete distributed procedure implementations.

As an illustration, the ``TableDataRewriteDistributedProcedure`` subtype, which handles table data rewrite operations, is defined as follows:

.. code-block:: java

   public class TableDataRewriteDistributedProcedure
           extends DistributedProcedure
   {
       private final BeginCallDistributedProcedure beginCallDistributedProcedure;
       private final FinishCallDistributedProcedure finishCallDistributedProcedure;
       private Supplier<ConnectorProcedureContext> contextSupplier;

       public TableDataRewriteDistributedProcedure(String schema, String name,
                                                   List<Argument> arguments,
                                                   BeginCallDistributedProcedure beginCallDistributedProcedure,
                                                   FinishCallDistributedProcedure finishCallDistributedProcedure,
                                                   Supplier<ConnectorProcedureContext> contextSupplier)
       {
           super(TABLE_DATA_REWRITE, schema, name, arguments);
           this.beginCallDistributedProcedure = requireNonNull(beginCallDistributedProcedure, "beginCallDistributedProcedure is null");
           this.finishCallDistributedProcedure = requireNonNull(finishCallDistributedProcedure, "finishCallDistributedProcedure is null");
           this.contextSupplier = requireNonNull(contextSupplier, "contextSupplier is null");

           // Performs subtype-specific validation and processing logic on the parameters
           ......
       }

       @Override
       public ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments)
       {
           return this.beginCallDistributedProcedure.begin(session, procedureContext, tableLayoutHandle, arguments);
       }

       @Override
       public void finish(ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
       {
           this.finishCallDistributedProcedure.finish(procedureContext, procedureHandle, fragments);
       }

       @Override
       public ConnectorProcedureContext createContext()
       {
           return contextSupplier.get();
       }

       @FunctionalInterface
       public interface BeginCallDistributedProcedure
       {
           ConnectorDistributedProcedureHandle begin(ConnectorSession session, ConnectorProcedureContext procedureContext, ConnectorTableLayoutHandle tableLayoutHandle, Object[] arguments);
       }

       @FunctionalInterface
       public interface FinishCallDistributedProcedure
       {
           void finish(ConnectorProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments);
       }
   }

3. Processing of Subtypes in the Analysis Phase
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``visitCall(...)`` method of ``StatementAnalyzer``, add a branch to handle the newly defined subtypes, such as ``TABLE_DATA_REWRITE``:

.. code-block:: java

   @Override
   protected Scope visitCall(Call call, Optional<Scope> scope)
   {
       QualifiedObjectName procedureName = analysis.getProcedureName()
               .orElse(createQualifiedObjectName(session, call, call.getName(), metadata));
       ConnectorId connectorId = metadata.getCatalogHandle(session, procedureName.getCatalogName())
               .orElseThrow(() -> new SemanticException(MISSING_CATALOG, call, "Catalog %s does not exist", procedureName.getCatalogName()));

       if (!metadata.getProcedureRegistry().isDistributedProcedure(connectorId, toSchemaTableName(procedureName))) {
           throw new SemanticException(PROCEDURE_NOT_FOUND, "Distributed procedure not registered: " + procedureName);
       }
       DistributedProcedure procedure = metadata.getProcedureRegistry().resolveDistributed(connectorId, toSchemaTableName(procedureName));
       Object[] values = extractParameterValuesInOrder(call, procedure, metadata, session, analysis.getParameters());

       analysis.setUpdateInfo(call.getUpdateInfo());
       analysis.setDistributedProcedureType(Optional.of(procedure.getType()));
       analysis.setProcedureArguments(Optional.of(values));
       switch (procedure.getType()) {
           case TABLE_DATA_REWRITE:
               TableDataRewriteDistributedProcedure tableDataRewriteDistributedProcedure = (TableDataRewriteDistributedProcedure) procedure;

               // Performs analysis on the tableDataRewriteDistributedProcedure
               ......

               break;
           default:
               throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Unsupported distributed procedure type: " + procedure.getType());
       }
       return createAndAssignScope(call, scope, Field.newUnqualified(Optional.empty(), "rows", BIGINT));
   }

4. Processing of Subtypes in the Planning Phase
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``planStatementWithoutOutput(...)`` method of ``LogicalPlanner``, when the statement type is Call, add a branch to handle newly defined subtypes
such as ``TABLE_DATA_REWRITE``:

.. code-block:: java

   private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement)
   {
       ......
       else if (statement instanceof Call) {
           checkState(analysis.getDistributedProcedureType().isPresent(), "Call distributed procedure analysis is missing");
           switch (analysis.getDistributedProcedureType().get()) {
               case TABLE_DATA_REWRITE:
                   return createCallDistributedProcedurePlanForTableDataRewrite(analysis, (Call) statement);
               default:
                   throw new PrestoException(NOT_SUPPORTED, "Unsupported distributed procedure type: " + analysis.getDistributedProcedureType().get());
           }
       }
       else {
           throw new PrestoException(NOT_SUPPORTED, "Unsupported statement type " + statement.getClass().getSimpleName());
       }
   }

   private RelationPlan createCallDistributedProcedurePlanForTableDataRewrite(Analysis analysis, Call statement)
   {
       // Builds the logical plan for the table data rewrite procedure subtype from the analysis results:
       // TableScanNode → FilterNode → CallDistributedProcedureNode → TableFinishNode → OutputNode
       ......
   }

.. note::

   If a custom plan node is required, it must subsequently be handled in the plan visitors, the optimizers, and the local execution planner. If a custom
   local execution operator ultimately needs to be generated, it must be implemented within PrestoDB as well. (This part is beyond the scope of this document
   and will not be elaborated further)

Implementing a Concrete Distributed Procedure in a Specific Connector
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to normal procedures, PrestoDB uses the Guice dependency injection framework to manage the registration and lifecycle of distributed procedures,
enabling connectors to dynamically provide these callable distributed procedures to the engine. A concrete distributed procedure is implemented and bound
as a ``Provider<DistributedProcedure>``, which ensures an instance is created on-demand when a procedure needs to be executed. The following steps will
guide you through implementing and supplying a distributed procedure in your connector.

1. Procedure Provider Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~

An implementation class must implement the ``Provider<DistributedProcedure>`` interface. In its constructor, it can use @Inject to receive any Guice-managed
dependencies. The class is required to implement the ``get()`` method from the Provider interface, which is responsible for constructing and returning a
specific subclass instance of ``DistributedProcedure``.

2. Creation of a DistributedProcedure Instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The parameters required to create a ``DistributedProcedure`` subclass differ, but the following common parameters are mandatory and consistent with those
described in the normal ``Procedure`` above.

* ``String schema`` - The schema namespace to which this procedure belongs (typically ``system`` in PrestoDB)
* ``String name`` - The name of this procedure, for example, ``expire_snapshots``
* ``List<Argument> arguments`` - The parameter declarations list for this procedure

The following code demonstrates how to implement ``rewrite_data_files`` for the Iceberg connector, based on the ``TableDataRewriteDistributedProcedure`` class:

.. code-block:: java

   public class RewriteDataFilesProcedure
           implements Provider<DistributedProcedure>
   {
       TypeManager typeManager;
       JsonCodec<CommitTaskData> commitTaskCodec;

       @Inject
       public RewriteDataFilesProcedure(
               TypeManager typeManager,
               JsonCodec<CommitTaskData> commitTaskCodec)
       {
           this.typeManager = requireNonNull(typeManager, "typeManager is null");
           this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
       }

       @Override
       public DistributedProcedure get()
       {
           return new TableDataRewriteDistributedProcedure(
                   "system",
                   "rewrite_data_files",
                   ImmutableList.of(
                           new Argument(SCHEMA, VARCHAR),
                           new Argument(TABLE_NAME, VARCHAR),
                           new Argument("filter", VARCHAR, false, "TRUE"),
                           new Argument("options", "map(varchar, varchar)", false, null)),
                   (session, procedureContext, tableLayoutHandle, arguments) -> beginCallDistributedProcedure(session, (IcebergProcedureContext) procedureContext, (IcebergTableLayoutHandle) tableLayoutHandle, arguments),
                   ((procedureContext, tableHandle, fragments) -> finishCallDistributedProcedure((IcebergProcedureContext) procedureContext, tableHandle, fragments)),
                   IcebergProcedureContext::new);
       }

       private ConnectorDistributedProcedureHandle beginCallDistributedProcedure(ConnectorSession session, IcebergProcedureContext procedureContext, IcebergTableLayoutHandle layoutHandle, Object[] arguments)
       {
           try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
               Table icebergTable = procedureContext.getTable().orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
               IcebergTableHandle tableHandle = layoutHandle.getTable();

               // Performs the preparatory work required when starting the execution of ``rewrite_data_files``,
               // and encapsulates the necessary information and handling logic within the ``procedureContext``
               ......

               return new IcebergDistributedProcedureHandle(
                       tableHandle.getSchemaName(),
                       tableHandle.getIcebergTableName(),
                       toPrestoSchema(icebergTable.schema(), typeManager),
                       toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                       getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                       icebergTable.location(),
                       getFileFormat(icebergTable),
                       getCompressionCodec(session),
                       icebergTable.properties());
           }
       }

       private void finishCallDistributedProcedure(IcebergProcedureContext procedureContext, ConnectorDistributedProcedureHandle procedureHandle, Collection<Slice> fragments)
       {
           if (fragments.isEmpty() &&
                   procedureContext.getScannedDataFiles().isEmpty() &&
                   procedureContext.getFullyAppliedDeleteFiles().isEmpty()) {
               return;
           }

           try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
               IcebergDistributedProcedureHandle handle = (IcebergDistributedProcedureHandle) procedureHandle;
               Table icebergTable = procedureContext.getTransaction().table();

               // Performs the final atomic commit by leveraging Iceberg's `RewriteFiles` API.
               // This integrates the commit information from the distributed tasks (in `commitTasks`)
               // with the file change tracking (for example, `scannedDataFiles`, `fullyAppliedDeleteFiles`, `newFiles`)
               // maintained within the `procedureContext`.
               List<CommitTaskData> commitTasks = fragments.stream()
                       .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                       .collect(toImmutableList());
               ......

               RewriteFiles rewriteFiles = procedureContext.getTransaction().newRewrite();
               Set<DataFile> scannedDataFiles = procedureContext.getScannedDataFiles();
               Set<DeleteFile> fullyAppliedDeleteFiles = procedureContext.getFullyAppliedDeleteFiles();
               rewriteFiles.rewriteFiles(scannedDataFiles, fullyAppliedDeleteFiles, newFiles, ImmutableSet.of());

               ......
               rewriteFiles.commit();
           }
       }
   }

3. Exposing the DistributedProcedure Provider to PrestoDB
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the Guice binding module of your target connector, add a binding for the distributed procedure provider class defined above:

.. code-block:: java

   Multibinder<BaseProcedure<?>> procedures = newSetBinder(binder, new TypeLiteral<BaseProcedure<?>>() {});
   procedures.addBinding().toProvider(RewriteDataFilesProcedure.class).in(Scopes.SINGLETON);
   ......

During startup, the PrestoDB engine collects the distributed procedure providers the same way as normal procedure providers exposed by
all connectors and maintains them within their respective namespaces (for example, ``hive.system`` or ``iceberg.system``). Once startup is complete, users
can invoke these distributed procedures by specifying the corresponding connector namespace, for example:

.. code-block:: sql

   call iceberg.system.rewrite_data_files('default', 'test_table');
   call iceberg.system.rewrite_data_files(table_name => 'test_table', schema => 'default', filter => 'c1 > 3');