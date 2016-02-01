create table sensor_types(
    id bigint NOT NULL,
    id_sensor_unit bigint  NOT NULL,
    type varchar(8),
	description varchar(32),
	constraint pk_sensor_types primary key(id)
);

create table sensor_units(
	id bigint NOT NULL,
	type varchar(8),
	constraint pk_sensor_units primary key(id)
);

create table sensors(
	id_greenhouse bigint NOT NULL,
	id_sensor_type bigint NOT NULL,
	sensor_value real NOT NULL,
	received_time timestamp  NOT NULL,
    constraint pk_sensor primary key(id_greenhouse, id_sensor_type, received_time)
);

create table greenhouses(
	id bigint NOT NULL,
    id_city bigint NOT NULL,
	name varchar(32),
	constraint pk_greenhouses primary key(id)
);

create table cities(
    id bigint NOT NULL,
    id_county bigint NOT NULL,
    name varchar(64),
    constraint pk_cities primary key(id)
);

create table counties(
    id bigint NOT NULL,
    id_country bigint NOT NULL,
    name varchar(64),
    constraint pk_counties primary key(id)
);

create table countries(
    id bigint NOT NULL,
    name varchar(64),
    constraint pk_countries primary key(id)
);

alter table sensors add constraint fk_sensors__greenhouses foreign key(id_greenhouse) references greenhouses(id);
alter table sensor_types add constraint fk_sensor_types__sensor_units foreign key(id_sensor_unit) references sensor_units(id);
alter table sensors add constraint fk_sensors__sensor_types foreign key(id_sensor_type) references sensor_types(id);
alter table greenhouses add constraint fk_greenhouses__cities foreign key(id_city) references cities(id);
alter table cities add constraint fk_cities__counties foreign key(id_county) references counties(id);
alter table counties add constraint fk_counties__countries foreign key(id_country) references countries(id);




