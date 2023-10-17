create table room (
	id bigint primary key,
	name varchar(30) not null
);

create table student (
	id bigint primary key,
	name varchar(30) not null,
	sex char(1),
	room_id bigint not null,
	birthday timestamp,
	Check (sex in ( 'F', 'M')),
	FOREIGN KEY (room_id) REFERENCES room(id)
);
