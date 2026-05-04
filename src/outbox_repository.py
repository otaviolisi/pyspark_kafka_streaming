def get_pending_events(conn, batch_size: int):
    query = f"""
        SELECT TOP ({batch_size})
            event_id,
            movement_id,
            product_id,
            warehouse_id,
            quantity,
            event_type,
            created_at,
            status
        FROM inventory_events_outbox
        WHERE status = 'PENDING'
        ORDER BY created_at ASC;
    """

    cursor = conn.cursor()
    cursor.execute(query)

    columns = [column[0] for column in cursor.description]

    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def mark_as_published(conn, event_id: int):
    query = """
        UPDATE inventory_events_outbox
        SET status = 'PUBLISHED',
            published_at = GETDATE()
        WHERE event_id = ?;
    """

    cursor = conn.cursor()
    cursor.execute(query, event_id)
    conn.commit()


def mark_as_error(conn, event_id: int):
    query = """
        UPDATE inventory_events_outbox
        SET status = 'ERROR'
        WHERE event_id = ?;
    """

    cursor = conn.cursor()
    cursor.execute(query, event_id)
    conn.commit()