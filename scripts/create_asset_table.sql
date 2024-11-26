-- Table: public.assets

-- DROP TABLE IF EXISTS public.assets;

CREATE TABLE IF NOT EXISTS public.assets
(
    code text COLLATE pg_catalog."default" NOT NULL,
    issuer text COLLATE pg_catalog."default" NOT NULL,
    asset_type text COLLATE pg_catalog."default" NOT NULL,
    auth_required boolean,
    auth_revocable boolean,
    auth_immutable boolean,
    auth_clawback boolean,
    is_valid boolean,
    validation_error text COLLATE pg_catalog."default",
    last_valid timestamp without time zone,
    last_checked timestamp without time zone,
    display_decimals integer,
    home_domain text COLLATE pg_catalog."default",
    toml_url text COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    conditions text COLLATE pg_catalog."default",
    is_asset_anchored boolean,
    fixed_number integer,
    max_number integer,
    is_unlimited boolean,
    redemption_instructions text COLLATE pg_catalog."default",
    collateral_addresses text COLLATE pg_catalog."default",
    collateral_address_signatures text COLLATE pg_catalog."default",
    countries text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    operation_type text COLLATE pg_catalog."default",
    first_seen_ledger integer,
    last_updated timestamp with time zone,
    CONSTRAINT assets_pkey PRIMARY KEY (code, issuer)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.assets
    OWNER to postgres;
-- Index: idx_assets_first_seen_ledger

-- DROP INDEX IF EXISTS public.idx_assets_first_seen_ledger;

CREATE INDEX IF NOT EXISTS idx_assets_first_seen_ledger
    ON public.assets USING btree
    (first_seen_ledger ASC NULLS LAST)
    TABLESPACE pg_default;