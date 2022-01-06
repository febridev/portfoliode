import sqlalchemy
import pandas as pd

#Open Connection
connection_uri = "mariadb+mariadbconnector://root:demo@35.197.132.1:13306/dbdemo"

db_engine = sqlalchemy.create_engine(connection_uri)

datass = pd.read_sql("select grade_code,gradecategory_code,grade_name from teomjobgrade",db_engine)
aa = pd.DataFrame(datass)
bb = aa.values.tolist()
# grade_code = list(datass["grade_code"])
# gradecategory_code = list(datass["gradecategory_code"])

# cc = list.append(gradecategory_code,grade_code)
print(bb)
# datass["grade_code"]


# for ld in dc:
#     print(ld)



# Define the MariaDB engine using MariaDB Connector/Python
#engine = sqlalchemy.create_engine("mariadb+mariadbconnector://root:demo@35.197.132.1:13306/dbdemo")
#with engine.connect() as conn:
   # result = conn.execution_options(stream_results=True).execute(text("select * from teomjobgrade"))

# session = sqlalchemy.orm.sessionmaker()
# session.configure(bind=engine)
# session = session()
# base = declarative_base()
# base.metadata.create_all(engine)

# employees = session.query(teomjobgrade).all()