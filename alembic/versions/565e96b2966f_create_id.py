"""create id

Revision ID: 565e96b2966f
Revises: 4c7c14ed923f
Create Date: 2015-09-29 14:50:07.060815

"""

# revision identifiers, used by Alembic.
revision = '565e96b2966f'
down_revision = '4c7c14ed923f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    with op.batch_alter_table('cmip5') as batch_op:
        batch_op.add_column(sa.Column('id', sa.Integer()))
        batch_op.create_primary_key('id',['id'])


def downgrade():
    with op.batch_alter_table('cmip5') as batch_op:
        batch_op.drop_column('id')
