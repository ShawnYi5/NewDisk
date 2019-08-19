"""empty message

Revision ID: 4955c9a64c1c
Revises: 
Create Date: 2019-08-19 15:37:52.959368

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4955c9a64c1c'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('created_idents',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('created_ident', sa.String(length=32), nullable=False),
    sa.Column('safe_status', sa.String(length=2), nullable=True),
    sa.Column('open_status', sa.String(length=2), nullable=False),
    sa.Column('destroy_status', sa.String(length=2), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    # op.create_unique_constraint(None, 'snapshot_storage', ['ident'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_constraint(None, 'snapshot_storage', type_='unique')
    op.drop_table('created_idents')
    # ### end Alembic commands ###
