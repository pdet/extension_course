import duckdb

def test_anonymize():
    conn = duckdb.connect('');
    conn.execute("SELECT anonymize('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Anonymize Sam 🐥");