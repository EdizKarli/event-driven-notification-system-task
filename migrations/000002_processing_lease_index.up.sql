CREATE INDEX idx_notifications_processing_lease
    ON notifications (locked_until)
    WHERE status = 'processing';
