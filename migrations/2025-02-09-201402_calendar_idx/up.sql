CREATE INDEX chateau_calendar_by_service ON gtfs.calendar (chateau, service_id);
CREATE INDEX chateau_calendar_date_by_service ON gtfs.calendar_dates (chateau, service_id);