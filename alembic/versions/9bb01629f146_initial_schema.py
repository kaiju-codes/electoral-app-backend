"""initial_schema

Revision ID: 9bb01629f146
Revises: 
Create Date: 2025-12-05 22:35:08.272333

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '9bb01629f146'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enum types
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE extractionrunstatus AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL', 'SKIPPED');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE segmenttype AS ENUM ('HEADER', 'LIST_CHUNK');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE segmentstatus AS ENUM ('PENDING', 'RUNNING', 'DONE', 'FAILED', 'SKIPPED');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)
    
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE apikeyprovider AS ENUM ('GEMINI', 'GPT');
        EXCEPTION
            WHEN duplicate_object THEN null;
        END $$;
    """)

    # Create documents table
    op.create_table(
        'documents',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('original_filename', sa.String(length=512), nullable=False),
        sa.Column('upload_file_uri', sa.String(length=1024), nullable=True),
        sa.Column('mime_type', sa.String(length=255), nullable=True),
        sa.Column('page_count', sa.Integer(), nullable=True),
        sa.Column('page_size_kb', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_documents_id'), 'documents', ['id'], unique=False)

    # Create extraction_runs table
    op.create_table(
        'extraction_runs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('document_id', sa.Integer(), nullable=False),
        sa.Column('status', postgresql.ENUM('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL', 'SKIPPED', name='extractionrunstatus', create_type=False), nullable=False, server_default='PENDING'),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('error_message', sa.String(length=2000), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_extraction_runs_id'), 'extraction_runs', ['id'], unique=False)

    # Create extraction_segments table
    op.create_table(
        'extraction_segments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('extraction_run_id', sa.Integer(), nullable=False),
        sa.Column('segment_type', postgresql.ENUM('HEADER', 'LIST_CHUNK', name='segmenttype', create_type=False), nullable=False),
        sa.Column('page_start', sa.Integer(), nullable=False),
        sa.Column('page_end', sa.Integer(), nullable=False),
        sa.Column('status', postgresql.ENUM('PENDING', 'RUNNING', 'DONE', 'FAILED', 'SKIPPED', name='segmentstatus', create_type=False), nullable=False, server_default='PENDING'),
        sa.Column('raw_response_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('parsed_header_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('parsed_list_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['extraction_run_id'], ['extraction_runs.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_extraction_segments_id'), 'extraction_segments', ['id'], unique=False)

    # Create document_header table
    op.create_table(
        'document_header',
        sa.Column('document_id', sa.Integer(), nullable=False),
        sa.Column('state', sa.String(length=255), nullable=True),
        sa.Column('part_number', sa.Integer(), nullable=True),
        sa.Column('language', sa.String(length=50), nullable=True),
        sa.Column('assembly_constituency_number_local', sa.String(length=255), nullable=True),
        sa.Column('assembly_constituency_number_english', sa.Integer(), nullable=True),
        sa.Column('assembly_constituency_name_local', sa.String(length=255), nullable=True),
        sa.Column('assembly_constituency_name_english', sa.String(length=255), nullable=True),
        sa.Column('polling_station_number_local', sa.String(length=255), nullable=True),
        sa.Column('polling_station_number_english', sa.Integer(), nullable=True),
        sa.Column('polling_station_name_local', sa.String(length=255), nullable=True),
        sa.Column('polling_station_name_english', sa.String(length=255), nullable=True),
        sa.Column('polling_station_building_and_address_local', sa.String(length=1024), nullable=True),
        sa.Column('polling_station_building_and_address_english', sa.String(length=1024), nullable=True),
        sa.Column('raw_header_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ),
        sa.PrimaryKeyConstraint('document_id')
    )

    # Create voters table
    op.create_table(
        'voters',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('document_id', sa.Integer(), nullable=False),
        sa.Column('serial_number', sa.Integer(), nullable=True),
        sa.Column('house_number', sa.String(length=255), nullable=True),
        sa.Column('voter_name_local', sa.String(length=255), nullable=True),
        sa.Column('voter_name_english', sa.String(length=255), nullable=True),
        sa.Column('relation_type', sa.String(length=50), nullable=True),
        sa.Column('relation_name_local', sa.String(length=255), nullable=True),
        sa.Column('relation_name_english', sa.String(length=255), nullable=True),
        sa.Column('gender', sa.String(length=20), nullable=True),
        sa.Column('age', sa.Integer(), nullable=True),
        sa.Column('photo_id', sa.String(length=255), nullable=True),
        sa.Column('raw_row_json', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('document_id', 'serial_number', name='uq_voter_doc_serial')
    )
    op.create_index(op.f('ix_voters_id'), 'voters', ['id'], unique=False)

    # Create document_sections table
    op.create_table(
        'document_sections',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('document_id', sa.Integer(), nullable=False),
        sa.Column('section_id', sa.Integer(), nullable=False),
        sa.Column('section_name_local', sa.String(length=255), nullable=True),
        sa.Column('section_name_english', sa.String(length=255), nullable=True),
        sa.Column('start_serial_number', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['documents.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('document_id', 'section_id', name='uq_document_section')
    )
    op.create_index(op.f('ix_document_sections_id'), 'document_sections', ['id'], unique=False)

    # Create api_key_settings table
    op.create_table(
        'api_key_settings',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('provider_type', postgresql.ENUM('GEMINI', 'GPT', name='apikeyprovider', create_type=False), nullable=False),
        sa.Column('encrypted_api_key', sa.String(length=2000), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('provider_type', name='uq_api_key_provider')
    )
    op.create_index(op.f('ix_api_key_settings_id'), 'api_key_settings', ['id'], unique=False)


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table('api_key_settings')
    op.drop_table('document_sections')
    op.drop_table('voters')
    op.drop_table('document_header')
    op.drop_table('extraction_segments')
    op.drop_table('extraction_runs')
    op.drop_table('documents')

    # Drop enum types
    op.execute('DROP TYPE IF EXISTS apikeyprovider')
    op.execute('DROP TYPE IF EXISTS segmentstatus')
    op.execute('DROP TYPE IF EXISTS segmenttype')
    op.execute('DROP TYPE IF EXISTS extractionrunstatus')
