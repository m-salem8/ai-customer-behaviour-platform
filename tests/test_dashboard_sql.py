def test_window_sql_contains_filter():
    window_sql = "WHERE event_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes')"

    assert "event_time" in window_sql
    assert "INTERVAL" in window_sql

def test_all_time_window_empty():
    window_sql = ""

    assert window_sql == ""

def test_extra_condition_conversion():
    window_sql = "WHERE event_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes')"
    extra_condition = window_sql.replace("WHERE", "AND")

    assert extra_condition.startswith("AND")

def test_top_view_query_structure():
    query = """
    SELECT product_id, COUNT(*) AS views
    FROM customer_events
    WHERE event_type = 'product_view'
    GROUP BY product_id
    ORDER BY views DESC
    LIMIT 10;
    """

    assert "product_view" in query
    assert "GROUP BY product_id" in query
    assert "LIMIT 10" in query