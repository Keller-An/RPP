CREATE TABLE IF NOT EXISTS flights (
    id          bigserial PRIMARY KEY,
    carrier     text        NOT NULL,   -- перевозчик
    flight_no   text        NOT NULL,   -- номер рейса
    origin      varchar(3)  NOT NULL,   -- аэропорт вылета
    destination varchar(3)  NOT NULL,   -- аэропорт прилёта
    dep_time    timestamp   NOT NULL,   -- время вылета
    arr_time    timestamp   NOT NULL,   -- время прилёта
    price       integer     NOT NULL    -- цена, руб.
);
-- выборка рейсов за дату: dep_time >= d AND dep_time < d + 1 день
CREATE INDEX IF NOT EXISTS flights_dep_time_idx ON flights (dep_time);
