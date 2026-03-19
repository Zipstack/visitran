from typing import TYPE_CHECKING, Any

from visitran.adapters.postgres.connection import PostgresConnection
from visitran.adapters.scd import BaseSCD
from visitran.templates.snapshot import VisitranSnapshot

if TYPE_CHECKING:  # pragma: no cover
    from ibis.expr.types.relations import Table


class PostgresSCD(BaseSCD):
    def __init__(self, db_connection: PostgresConnection, visitran_scd: VisitranSnapshot):
        super().__init__(db_connection, visitran_scd)
        self._statements: list[Any] = []
        self._db_connection: PostgresConnection = db_connection

    @property
    def db_connection(self) -> PostgresConnection:
        return self._db_connection

    @staticmethod
    def get_snapshot_timestamp() -> str:
        return "now()::timestamp without time zone"

    def scd_timestamp_table(self) -> str:
        """Returns SQL schema for Timestamp based SCD table creation."""
        sql_query = f"""create table {self.target_sql} as (
            select *,
            {self.snapshot_hash_arguments([self.unique_id_attr, self.updated_at_attr])}
            as scd_id,
            {self.updated_at_attr} as scd_updated_at,
            {self.updated_at_attr} as scd_valid_from,
            nullif({self.updated_at_attr}, {self.updated_at_attr}) as scd_valid_to
            from (
                select * from {self.source_sql}
            ) sbq
        );
        """
        return sql_query

    def scd_check_table(self) -> str:
        """Returns SQL schema for Check based SCD table creation."""
        sql_query = f"""create table {self.target_sql} as (
            select *,
            {self.snapshot_hash_arguments([
            self.unique_id_attr,
            self.get_snapshot_timestamp()])}
            as scd_id,
            {self.get_snapshot_timestamp()} as scd_updated_at,
            {self.get_snapshot_timestamp()} as scd_valid_from,
            nullif(
                    {self.get_snapshot_timestamp()},
                    {self.get_snapshot_timestamp()}
                ) as scd_valid_to
            from (
                select * from {self.source_sql}
            ) sbq
        );
        """
        return sql_query

    def create_snapshot_staging_table(self) -> str:
        """Returns SQL schema for temporary staging table."""
        if self.scd_strategy in ("check",) and not self.updated_at_attr:
            self["updated_at"] = self.get_snapshot_timestamp()
        staging_query: str = f"""
        create temporary table "{self.create_temporary_table_name()}" as (
        {self._get_scd_query()}
        {self._get_snapshotted_data_query()}
        {self._get_insertion_source_query()}
        {self._get_update_source_query()}
        """
        if self.invalidate_hard_deletes:
            staging_query += self._get_deletes_source_query()

        staging_query += self._get_insertion_query()
        staging_query += self._get_updates_query()

        if self.invalidate_hard_deletes:
            staging_query += ", "
            staging_query += self._get_deletes_query()

        staging_query += """
        select * from insertions
        union all
        select * from updates
        """
        if self.invalidate_hard_deletes:
            staging_query += """
            union all
            select * from deletes
            """
        staging_query += "\n ); \n\n"
        return staging_query

    def _get_scd_query(self) -> str:
        """Refers Source table as snapshot_query."""
        return f"""
        with snapshot_query as (
            select * FROM {self.source_sql}
        ),
        """

    def _get_snapshotted_data_query(self) -> str:
        """Refers previously snapshotted data and retrivies the latest info in
        the name of snapshotted_data."""
        return f"""
        snapshotted_data as (
            select *,
                {self.unique_id_attr} as scd_unique_key
            from {self.target_sql}
            where scd_valid_to is null
        ),
        """

    def _get_insertion_source_query(self) -> str:
        """Refers newly added records from snapshot_query."""
        return f"""
        insertions_source_data as (
            select *,
            {self.unique_id_attr} as scd_unique_key,
            {self.updated_at_attr} as scd_updated_at,
            {self.updated_at_attr} as scd_valid_from,
            nullif({self.updated_at_attr}, {self.updated_at_attr}) as scd_valid_to,
            {self.snapshot_hash_arguments([self.unique_id_attr, self.updated_at_attr])}
            as scd_id
            from snapshot_query
        ),
        """

    def _get_update_source_query(self) -> str:
        """Refers updated datas from snapshot_query."""
        return f"""
        updates_source_data as (
            select *,
                {self.unique_id_attr} as scd_unique_key,
                {self.updated_at_attr} as scd_updated_at,
                {self.updated_at_attr} as scd_valid_from,
                {self.updated_at_attr} as scd_valid_to
            from snapshot_query
        ),
        """

    def _get_deletes_source_query(self) -> str:
        """Refers deleted datas from snapshot_query."""
        return f"""
        deletes_source_data as (
            select
                *,
                {self.unique_id_attr} as scd_unique_key
            from snapshot_query
        ),
        """

    def _get_insertion_query(self) -> str:
        """Refers insertion query from snapshotted_data."""
        return f"""
        insertions as (
             select
                'insert' as scd_change_type,
                source_data.*
            from insertions_source_data as source_data
            left outer join snapshotted_data on
                snapshotted_data.scd_unique_key = source_data.scd_unique_key
            where snapshotted_data.scd_unique_key is null
               or (
                    snapshotted_data.scd_unique_key is not null
                and (
                    {self.row_changed_expr}
                )
            )
        ),
        """

    def _get_updates_query(self) -> str:
        """Refers data to be updated in snapshot table."""
        return f"""
        updates as (
            select
                'update' as scd_change_type,
                source_data.*,
                snapshotted_data.scd_id
            from updates_source_data as source_data
            join snapshotted_data on
                snapshotted_data.scd_unique_key = source_data.scd_unique_key
            where (
                {self.row_changed_expr}
            )
        )
        """

    def _get_deletes_query(self) -> str:
        """Refers data to be deleted in snapshot table."""
        return f"""
        deletes as (
            select
                'delete' as scd_change_type,
                source_data.*,
                {self.get_snapshot_timestamp()} as scd_valid_from,
                {self.get_snapshot_timestamp()} as scd_updated_at,
                {self.get_snapshot_timestamp()} as scd_valid_to,
                snapshotted_data.scd_id
            from snapshotted_data
            left join deletes_source_data as source_data on
                snapshotted_data.scd_unique_key = source_data.scd_unique_key
            where source_data.scd_unique_key is null
        )
        """

    def merge_snapshot_staging_table(self, staging_cols: list[str]) -> str:
        """Returns SQL schema for merge staging tables."""
        cols = []
        internal_cols = []
        for _col in staging_cols:
            cols.append(f'"{_col}"')
            internal_cols.append(f'SCD_INTERNAL_SOURCE."{_col}"')
        merge_query = f"""
        update {self.target_sql}
        set scd_valid_to = SCD_INTERNAL_SOURCE.scd_Valid_to
        from {self.temporary_table} as SCD_INTERNAL_SOURCE
        where SCD_INTERNAL_SOURCE.scd_id::text = {self.target_sql}.scd_id::text
            and SCD_INTERNAL_SOURCE.scd_change_type::text in
                ('update'::text,'delete'::text)
            and {self.target_sql}.scd_valid_to is null;

        insert into {self.target_sql} ({", ".join(cols)})
        select {", ".join(internal_cols)}
        from {self.temporary_table} as SCD_INTERNAL_SOURCE
        where SCD_INTERNAL_SOURCE.scd_change_type::text = 'insert'::text;
        """
        return merge_query

    def execute(self) -> bool:
        # self._validate_snapshot_configuration()

        is_table_exist: bool = self.db_connection.is_table_exists(
            schema_name=self.snapshot_schema_name,
            table_name=self.snapshot_table_name,
        )

        # Creating initial snapshot table.
        if not is_table_exist:
            create_snp_query: str = self.create_snapshot_table()
            self._statements.append([create_snp_query])
            # Executing all the constructed SQL statements in adapters
            return self.db_connection.bulk_execute_statements(statements=self._statements)
        else:
            # Creating temporary staging table
            create_tmp_stg_table: str = self.create_snapshot_staging_table()
            self._statements.append([create_tmp_stg_table])

            # Identifying newly added coloumns
            source_cols = self.db_connection.get_table_columns(
                schema_name=self.source_schema_name, table_name=self.source_table_name
            )
            target_cols = self.db_connection.get_table_columns(
                schema_name=self.snapshot_schema_name,
                table_name=self.snapshot_table_name,
            )
            missing_cols = set(source_cols) - set(target_cols)

            # Adding missing columns in snapshotted table
            if missing_cols:
                source_table: Table = self.db_connection.get_table_obj(
                    schema_name=self.source_schema_name,
                    table_name=self.source_table_name,
                )
                for col in missing_cols:
                    col_type = source_table[col].type()
                    self._statements.append(
                        [
                            self.db_connection.add_column(
                                schema_name=self.snapshot_schema_name,
                                table_name=self.snapshot_table_name,
                                column_name=col,
                                column_type=col_type,
                            )
                        ]
                    )

            # Merge staging table data's with snapshot table
            staging_cols = set(source_cols + target_cols)
            self._statements.append([self.merge_snapshot_staging_table(list(staging_cols))])
            # Executing all the constructed SQL statements in adapters
            return self.db_connection.bulk_execute_statements(statements=self._statements)
