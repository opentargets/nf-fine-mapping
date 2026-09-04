"""Schema contracts for collector-produced datasets."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

ScalarType = Literal["VARCHAR", "INTEGER", "DOUBLE", "FLOAT", "BOOLEAN"]


class SchemaField(BaseModel):
    """A named field in a DuckDB/Parquet schema contract."""

    model_config = ConfigDict(frozen=True)

    name: str
    duckdb_type: ScalarType | str
    nullable: bool = True

    def sql_type(self) -> str:
        """Return this field's DuckDB SQL type."""
        return self.duckdb_type


class StructSchema(BaseModel):
    """Ordered struct schema used inside nested Parquet fields."""

    model_config = ConfigDict(frozen=True)

    fields: tuple[SchemaField, ...]

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return field names in schema order."""
        return tuple(field.name for field in self.fields)

    def sql_type(self) -> str:
        """Return a DuckDB STRUCT type expression."""
        fields_sql = ",\n        ".join(f"{field.name} {field.sql_type()}" for field in self.fields)
        return f"STRUCT(\n        {fields_sql}\n    )"


class ListSchema(BaseModel):
    """Ordered list schema for repeated nested Parquet fields."""

    model_config = ConfigDict(frozen=True)

    item_schema: StructSchema

    def sql_type(self) -> str:
        """Return a DuckDB list type expression."""
        return f"{self.item_schema.sql_type()}[]"


class DatasetField(BaseModel):
    """A top-level dataset field."""

    model_config = ConfigDict(frozen=True)

    name: str
    duckdb_type: ScalarType | str | ListSchema
    nullable: bool = True

    def sql_type(self) -> str:
        """Return this field's DuckDB SQL type."""
        if isinstance(self.duckdb_type, ListSchema):
            return self.duckdb_type.sql_type()
        return self.duckdb_type

    def null_select_sql(self) -> str:
        """Return a typed NULL select expression for schema-only writes."""
        return f"CAST(NULL AS {self.sql_type()}) AS {self.name}"


class DatasetSchema(BaseModel):
    """Ordered top-level schema for a collector dataset."""

    model_config = ConfigDict(frozen=True)

    fields: tuple[DatasetField, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return column names in schema order."""
        return tuple(field.name for field in self.fields)

    def empty_select_sql(self) -> str:
        """Return a DuckDB SELECT statement with no rows and this schema."""
        select_sql = ",\n    ".join(field.null_select_sql() for field in self.fields)
        return f"SELECT\n    {select_sql}\nWHERE false"


LOCUS_STRUCT_SCHEMA = StructSchema(
    fields=(
        SchemaField(name="is95CredibleSet", duckdb_type="BOOLEAN"),
        SchemaField(name="is99CredibleSet", duckdb_type="BOOLEAN"),
        SchemaField(name="logBF", duckdb_type="DOUBLE"),
        SchemaField(name="posteriorProbability", duckdb_type="DOUBLE"),
        SchemaField(name="variantId", duckdb_type="VARCHAR"),
        SchemaField(name="pValueMantissa", duckdb_type="FLOAT"),
        SchemaField(name="pValueExponent", duckdb_type="INTEGER"),
        SchemaField(name="beta", duckdb_type="DOUBLE"),
        SchemaField(name="standardError", duckdb_type="DOUBLE"),
        SchemaField(name="r2Overall", duckdb_type="DOUBLE"),
    )
)

COLLECTED_LOCUS_STRUCT_SCHEMA = StructSchema(
    fields=(
        SchemaField(name="variantId", duckdb_type="VARCHAR"),
        SchemaField(name="pValueMantissa", duckdb_type="FLOAT"),
        SchemaField(name="pValueExponent", duckdb_type="INTEGER"),
        SchemaField(name="beta", duckdb_type="DOUBLE"),
        SchemaField(name="standardError", duckdb_type="DOUBLE"),
    )
)

CANONICAL_REGION_INPUT_LOCUS_SCHEMA = StructSchema(
    fields=(
        SchemaField(name="studyId", duckdb_type="VARCHAR"),
        SchemaField(name="studyLocusId", duckdb_type="VARCHAR"),
        SchemaField(name="ancestry", duckdb_type="VARCHAR"),
    )
)

STUDY_LOCUS_SCHEMA = DatasetSchema(
    fields=(
        DatasetField(name="studyLocusId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="studyId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="variantId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="chromosome", duckdb_type="VARCHAR"),
        DatasetField(name="position", duckdb_type="INTEGER"),
        DatasetField(name="beta", duckdb_type="DOUBLE"),
        DatasetField(name="sampleSize", duckdb_type="INTEGER"),
        DatasetField(name="pValueMantissa", duckdb_type="FLOAT"),
        DatasetField(name="pValueExponent", duckdb_type="INTEGER"),
        DatasetField(name="effectAlleleFrequencyFromSource", duckdb_type="FLOAT"),
        DatasetField(name="standardError", duckdb_type="DOUBLE"),
        DatasetField(name="qualityControls", duckdb_type="VARCHAR[]"),
        DatasetField(name="locusStart", duckdb_type="INTEGER"),
        DatasetField(name="locusEnd", duckdb_type="INTEGER"),
        DatasetField(name="locus", duckdb_type=ListSchema(item_schema=LOCUS_STRUCT_SCHEMA)),
    )
)

COLLECTED_LOCUS_SCHEMA = DatasetSchema(
    fields=(
        DatasetField(name="fineMappingLocusSetId", duckdb_type="VARCHAR"),
        DatasetField(name="studyLocusId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="studyId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="chromosome", duckdb_type="VARCHAR"),
        DatasetField(name="locusStart", duckdb_type="INTEGER"),
        DatasetField(name="locusEnd", duckdb_type="INTEGER"),
        DatasetField(name="qualityControls", duckdb_type="VARCHAR[]"),
        DatasetField(name="locus", duckdb_type=ListSchema(item_schema=COLLECTED_LOCUS_STRUCT_SCHEMA)),
    )
)

CANONICAL_INPUT_LOCUS_SCHEMA = StructSchema(
    fields=(
        SchemaField(name="studyId", duckdb_type="VARCHAR"),
        SchemaField(name="studyLocusId", duckdb_type="VARCHAR"),
    )
)

CANONICAL_COMPONENT_SCHEMA = StructSchema(
    fields=(
        SchemaField(name="studyId", duckdb_type="VARCHAR"),
        SchemaField(name="studyLocusId", duckdb_type="VARCHAR"),
        SchemaField(name="nVariants", duckdb_type="INTEGER"),
        SchemaField(name="nVariantsBelowMafCutoff", duckdb_type="INTEGER"),
        SchemaField(name="qualityControls", duckdb_type="VARCHAR[]"),
    )
)

CANONICAL_REGION_STATS_SCHEMA = DatasetSchema(
    fields=(
        DatasetField(name="fineMappingLocusSetId", duckdb_type="VARCHAR"),
        DatasetField(name="chromosome", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="locusStart", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="locusEnd", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="nVariants", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="nVariantsAboveMafCutoff", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="inputLoci", duckdb_type=ListSchema(item_schema=CANONICAL_INPUT_LOCUS_SCHEMA), nullable=False),
        DatasetField(name="components", duckdb_type=ListSchema(item_schema=CANONICAL_COMPONENT_SCHEMA), nullable=False),
        DatasetField(name="nIntersectionVariants", duckdb_type="INTEGER"),
        DatasetField(name="nUnionVariants", duckdb_type="INTEGER"),
        DatasetField(name="variantOverlapProportion", duckdb_type="DOUBLE"),
        DatasetField(name="minimumVariantOverlapProportion", duckdb_type="DOUBLE", nullable=False),
        DatasetField(name="qualityControls", duckdb_type="VARCHAR[]", nullable=False),
    )
)

CANONICAL_REGION_SCHEMA = DatasetSchema(
    fields=(
        DatasetField(name="canonicalRegionId", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="chromosome", duckdb_type="VARCHAR", nullable=False),
        DatasetField(name="regionStart", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="regionEnd", duckdb_type="INTEGER", nullable=False),
        DatasetField(name="qualityControls", duckdb_type="VARCHAR[]", nullable=False),
        DatasetField(name="inputLoci", duckdb_type=ListSchema(item_schema=CANONICAL_REGION_INPUT_LOCUS_SCHEMA), nullable=False),
    )
)
