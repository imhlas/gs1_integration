"""
Yksinkertaiset apufunktiot Azure SQL -kirjoituksiin ja -lukuihin Sparkilla.

Funktiot:
- write_append(df, table, dbutils, batchsize=None)
- write_overwrite(df, table, dbutils, truncate=True, batchsize=None)
- read_table(spark, table, dbutils, columns=None, top_n=None, order_by=None, to_pandas=False)
"""

def get_azure_sql_options(dbutils):
    return {
        "url": f"jdbc:sqlserver://{dbutils.secrets.get('gs1-kv', 'azuresql-server')}.database.windows.net:1433;"
               f"database={dbutils.secrets.get('gs1-kv', 'azuresql-database')}",
        "user": dbutils.secrets.get("gs1-kv", "azuresql-username"),
        "password": dbutils.secrets.get("gs1-kv", "azuresql-password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

def _get_jdbc_params(dbutils):
    """Hae JDBC-parametrit Key Vaultin kautta ja muodosta URL."""
    opts = get_azure_sql_options(dbutils)
    jdbc_url = opts["url"] + ";encrypt=true;trustServerCertificate=false;loginTimeout=30;"
    return jdbc_url, opts["user"], opts["password"], opts["driver"]


def write_append(df, table: str, dbutils, batchsize: int | None = None) -> None:
    """Lisää rivit tauluun (INSERT) append-tilassa."""
    jdbc_url, user, password, driver = _get_jdbc_params(dbutils)
    writer = (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
    )
    if batchsize:
        writer = writer.option("batchsize", str(batchsize))
    writer.mode("append").save()


def write_overwrite(df, table: str, dbutils, truncate: bool = True, batchsize: int | None = None) -> None:
    """
    Kirjoita tauluun overwrite-tilassa.
    truncate=True yrittää käyttää TRUNCATE TABLE -tapaa säilyttäen skeeman (tyypillinen täyspäivitys).
    """
    jdbc_url, user, password, driver = _get_jdbc_params(dbutils)
    writer = (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
    )
    if batchsize:
        writer = writer.option("batchsize", str(batchsize))
    if truncate:
        writer = writer.option("truncate", "true")
    writer.mode("overwrite").save()


def read_table(
    spark,
    table: str,
    dbutils,
    columns: list[str] | None = None,
    top_n: int | None = None,
    order_by: str | None = None,
    to_pandas: bool = False,
):
    """
    Lue taulusta DataFrameksi. Voit valita sarakkeet, TOP N -rajan ja ORDER BYn.
    Palauttaa Spark DF:n (oletus) tai Pandas DF:n, jos to_pandas=True.
    """
    jdbc_url, user, password, driver = _get_jdbc_params(dbutils)

    cols = ", ".join(columns) if columns else "*"
    top_clause = f"TOP {int(top_n)} " if top_n else ""
    order_clause = f" ORDER BY {order_by}" if order_by else ""
    query = f"SELECT {top_clause}{cols} FROM {table}{order_clause}"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", driver)
        .option("user", user)
        .option("password", password)
        .option("query", query)
        .load()
    )
    return df.toPandas() if to_pandas else df

