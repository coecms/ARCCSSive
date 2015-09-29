"""rename id to path

Revision ID: 4c7c14ed923f
Revises: 355a156d4b58
Create Date: 2015-09-29 14:46:38.785649

"""

# revision identifiers, used by Alembic.
revision = '4c7c14ed923f'
down_revision = '355a156d4b58'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    with op.batch_alter_table('cmip5') as batch_op:
        batch_op.alter_column('id',new_column_name='path')

def downgrade():
    with op.batch_alter_table('cmip5') as batch_op:
        batch_op.alter_column('path',new_column_name='id')
