-- 集成测试专用 PG schema（internal/it）。
-- 仅覆盖 points.PGLoader 读取所需列的最小子集，列名/类型与 ddl.md §4 建表一致；
-- 权威 PG 迁移链在 thermio-platform 仓（IMPL-3），此处不为测试复刻全量 DDL
-- （无 RLS/角色——RLS 是平台侧关注点，管道只经只读角色读两张表）。
CREATE TABLE IF NOT EXISTS tenant (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS building (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id  uuid NOT NULL REFERENCES tenant(id),
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gateway (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id      uuid NOT NULL REFERENCES tenant(id),
  building_id    uuid NOT NULL REFERENCES building(id),
  name           text NOT NULL,
  serial         text NOT NULL UNIQUE,
  mqtt_client_id text NOT NULL UNIQUE,
  status         text NOT NULL DEFAULT 'offline',
  last_seen_at   timestamptz,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS point (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tenant_id        uuid NOT NULL REFERENCES tenant(id),
  building_id      uuid NOT NULL REFERENCES building(id),
  source_type      text NOT NULL DEFAULT 'mqtt_gateway',
  gateway_id       uuid REFERENCES gateway(id),
  raw_name         text NOT NULL,
  unit_raw         text,
  unit_std         text,
  stale_timeout_s  int NOT NULL DEFAULT 300,
  valid_range_min  numeric,
  valid_range_max  numeric,
  status           text NOT NULL DEFAULT 'active',
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS point_gateway_raw_name_uidx
  ON point (gateway_id, raw_name) WHERE gateway_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS point_updated_at_idx ON point (updated_at);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS
$$ BEGIN NEW.updated_at = now(); RETURN NEW; END $$;
DROP TRIGGER IF EXISTS point_touch ON point;
CREATE TRIGGER point_touch BEFORE UPDATE ON point FOR EACH ROW EXECUTE FUNCTION set_updated_at();
