begin;

do $$
begin
	if not exists (select 1 from pg_type where typname = 'addr_type') then
		create type addr_type as enum ('EOA', 'Contract', 'Unknown');
	end if;

	if not exists (select 1 from pg_type where typname = 'token_type') then
		create type token_type as enum ('TRX', 'TRC10', 'TRC20');
	end if;

	if not exists (select 1 from pg_type where typname = 'transaction_type') then
		create type transaction_type as enum ('TransferContract', 'TransferAssetContract', 'CustomContract', 'TriggerSmartContract');
	end if;
end
$$;

create table if not exists addresses (
	id bigserial primary key,
	addr bytea not null unique,
	addr_t addr_type not null
);

create table if not exists tokens (
	id bigserial primary key,
	asset_id bigint unique,
	contract_addr bigint unique references addresses(id),
	token_t token_type not null
);
insert into tokens (id, asset_id, contract_addr, token_t) VALUES (0, NULL, NULL, 'TRX') ON CONFLICT DO NOTHING;

create table if not exists transactions (
	id bigint primary key, -- = block number * 1000 + transaction index in block

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
	
	token int not null references tokens(id),
	value_lo bigint not null,
	value_hi bigint not null,
	from_addr bigint not null references addresses(id),
	to_addr bigint not null references addresses(id),
	success bool,
	
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