"""update_section_constraint_for_multiple_occurrences

Revision ID: fc577ba2e566
Revises: 9bb01629f146
Create Date: 2025-12-14 15:50:15.341353

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fc577ba2e566'
down_revision: Union[str, None] = '9bb01629f146'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: Drop existing unique constraint
    op.drop_constraint('uq_document_section', 'document_sections', type_='unique')
    
    # Step 2: Add new unique constraint on (document_id, section_id, start_serial_number)
    # Note: In PostgreSQL, NULL values are considered distinct in unique constraints,
    # so multiple rows with NULL start_serial_number are allowed. We'll handle
    # NULL uniqueness in application logic if needed.
    op.create_unique_constraint(
        'uq_document_section_occurrence',
        'document_sections',
        ['document_id', 'section_id', 'start_serial_number']
    )
    
    # Step 3: Add partial unique index to ensure only one NULL start_serial_number
    # per (document_id, section_id) combination
    op.execute("""
        CREATE UNIQUE INDEX uq_document_section_null_start_serial
        ON document_sections (document_id, section_id)
        WHERE start_serial_number IS NULL
    """)
    
    # Step 4: Add index on (document_id, start_serial_number) for efficient range queries
    op.create_index(
        'ix_document_sections_doc_start_serial',
        'document_sections',
        ['document_id', 'start_serial_number']
    )


def downgrade() -> None:
    # Step 1: Drop the new index
    op.drop_index('ix_document_sections_doc_start_serial', table_name='document_sections')
    
    # Step 2: Drop the partial unique index
    op.execute("DROP INDEX IF EXISTS uq_document_section_null_start_serial")
    
    # Step 3: Drop the new unique constraint
    op.drop_constraint('uq_document_section_occurrence', 'document_sections', type_='unique')
    
    # Step 4: Restore old unique constraint
    op.create_unique_constraint(
        'uq_document_section',
        'document_sections',
        ['document_id', 'section_id']
    )
