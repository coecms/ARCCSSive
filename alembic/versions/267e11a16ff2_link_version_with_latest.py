"""link version with latest

Revision ID: 267e11a16ff2
Revises: 565e96b2966f
Create Date: 2015-09-29 15:31:01.371315

"""

# revision identifiers, used by Alembic.
revision = '267e11a16ff2'
down_revision = '565e96b2966f'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    with op.batch_alter_table('version') as batch_op:
        batch_op.add_column(sa.Column('latest_id', sa.Integer(), nullable=True))
        batch_op.create_foreign_key('fk_latest_id', 'cmip5', ['latest_id'], ['id'])


def downgrade():
    with op.batch_alter_table('version') as batch_op:
        batch_op.drop_constraint('fk_latest_id', type_='foreignkey')
        batch_op.drop_column('latest_id')
