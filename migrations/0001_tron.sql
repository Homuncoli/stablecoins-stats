begin;

create table if not exists blocks (
	height bigint primary key,
	producer bytea not null,
	ts timestamp not null
);

create table if not exists transactions (
	hash bytea primary key,
	block bigint references blocks(height),
	result bool,
	ts timestamp not null,
	
	expiration timestamp,
	fee_limit bigint,
	type text,
	value jsonb,
	fee bigint,
	contract_address bytea,
	energy_usage bigint,
	net_fee bigint
);

create table if not exists logs (
	transaction bytea not null references transactions(hash),
	index int not null,
	
	address bytea ,
	topic0 bytea,
	topic1 bytea,
	topic2 bytea,
	topic3 bytea,
	data   bytea,
	
	primary key (transaction, index)
);

create table if not exists internal_transactions (
	transaction bytea not null references transactions(hash),
	index int not null,
	
	caller bytea ,
	transer_to     bytea ,
	note   text,
	rejected bool,
	
	primary key (transaction, index)
);

create table if not exists call_value_info (
	transaction bytea not null references transactions(hash),
	internal int not null,
	index int not null,
	
	value bigint not null,
	token bigint,
	
	primary key (transaction, internal, index)
);
commit;