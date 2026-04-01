begin;

create type addr_type as enum ('EOA', 'Contract');
create type token_type as enum ('TRX', 'TRC10', 'TRC20');
create type transaction_type as enum ('TransferContract', 'TransferAssetContract', 'CustomContract', 'TriggerSmartContract');
create type transfer_type as enum ('Transaction', 'Internal Transaction', 'Log');

create table if not exists addresses (
	id serial primary key,
	addr bytea not null unique,
	addr_t addr_type not null
);

create table if not exists token (
	id serial primary key,
	asset_name bytea unique,
	contract_addr int references addresses(id),
	token_t token_type not null
);
insert into token (id, asset_name, contract_addr, token_t) VALUES (0, NULL, NULL, 'TRX') ON CONFLICT DO NOTHING;

create table if not exists transactions (
	id bigint primary key,

	block bigint not null,
	result bool,
	ts timestamp not null,
	transaction_t transaction_type not null,
	
	fee_limit bigint,
	fee bigint,
	energy_usage bigint,
	net_fee bigint
);

create table if not exists transfers (
	transaction bigint not null references transactions(id),
	index smallint not null,
	transfer_t transfer_type not null,
	
	token int not null references token(id),
	value bigint not null,
	to_addr int not null references addresses(id),
	from_addr int not null references addresses(id),
	rejected bool,
	
	primary key (transaction, index)
);


-- All NOT transfer logs
create table if not exists logs (
	transaction bigint not null references transactions(id),
	index smallint not null,
	
	address bytea ,
	topic0 bytea,
	topic1 bytea,
	topic2 bytea,
	topic3 bytea,
	data   bytea,
	
	primary key (transaction, index)
);
commit;