"""add constraint

Revision ID: 2b79334818ff
Revises: 267e11a16ff2
Create Date: 2015-09-30 10:37:04.673197

"""

# revision identifiers, used by Alembic.
revision = '2b79334818ff'
down_revision = '267e11a16ff2'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    with op.batch_alter_table('variable') as batch_op:
        batch_op.create_unique_constraint('constr_variable', ['variable', 'experiment', 'mip', 'model', 'ensemble'])


def downgrade():
    with op.batch_alter_table('variable') as batch_op:
        batch_op.drop_constraint('constr_variable', type_='unique')
