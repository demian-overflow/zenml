"""Add parent run and pipeline outputs [c2f8d07a91b4].

Revision ID: c2f8d07a91b4
Revises: 0.94.3
Create Date: 2026-04-27 15:20:00.000000

"""

import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision = "c2f8d07a91b4"
down_revision = "0.94.3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade database schema and/or data, creating a new revision."""
    with op.batch_alter_table("pipeline_run", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "parent_run_id",
                sqlmodel.sql.sqltypes.GUID(),
                nullable=True,
            )
        )
        batch_op.create_foreign_key(
            "fk_pipeline_run_parent_run_id_pipeline_run",
            "pipeline_run",
            ["parent_run_id"],
            ["id"],
            ondelete="SET NULL",
        )

    op.create_table(
        "pipeline_run_output",
        sa.Column("id", sqlmodel.sql.sqltypes.GUID(), nullable=False),
        sa.Column("created", sa.DateTime(), nullable=False),
        sa.Column("updated", sa.DateTime(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "pipeline_run_id",
            sqlmodel.sql.sqltypes.GUID(),
            nullable=False,
        ),
        sa.Column("artifact_id", sqlmodel.sql.sqltypes.GUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["pipeline_run_id"],
            ["pipeline_run.id"],
            name="fk_pipeline_run_output_pipeline_run_id_pipeline_run",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["artifact_version.id"],
            name="fk_pipeline_run_output_artifact_id_artifact_version",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "pipeline_run_id",
            "name",
            name="unique_pipeline_run_output_name",
        ),
    )


def downgrade() -> None:
    """Downgrade database schema and/or data back to the previous revision."""
    op.drop_table("pipeline_run_output")

    with op.batch_alter_table("pipeline_run", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_pipeline_run_parent_run_id_pipeline_run",
            type_="foreignkey",
        )
        batch_op.drop_column("parent_run_id")
