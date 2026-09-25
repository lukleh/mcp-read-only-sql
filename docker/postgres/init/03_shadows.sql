-- Names planted in public that shadow pg_catalog ones. The read-only guard's
-- shadow check must refuse bare calls to them (see test_security_readonly_integration).
CREATE FUNCTION public.md5(text) RETURNS text LANGUAGE sql AS $$ SELECT 'shadowed' $$;
CREATE FUNCTION public.shadow_op(int, int) RETURNS int LANGUAGE sql AS $$ SELECT 0 $$;
CREATE OPERATOR public.@@@ (LEFTARG = int, RIGHTARG = int, FUNCTION = public.shadow_op);
