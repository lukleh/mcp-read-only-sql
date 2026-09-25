-- Names planted outside pg_catalog that the read-only guard's shadow check
-- must catch (see test_security_readonly_integration).
--   public.md5(text)        exact shadow of a catalog function
--   public.upper(integer)   a different overload of a catalog function
--   public.@@@              a user operator
--   review_helper()         defined in two schemas, with "shadow" ahead of
--                           "public" on testuser's search_path
CREATE FUNCTION public.md5(text) RETURNS text LANGUAGE sql AS $$ SELECT 'shadowed' $$;
CREATE FUNCTION public.upper(integer) RETURNS text LANGUAGE sql AS $$ SELECT 'shadowed' $$;
CREATE FUNCTION public.shadow_op(int, int) RETURNS int LANGUAGE sql AS $$ SELECT 0 $$;
CREATE OPERATOR public.@@@ (LEFTARG = int, RIGHTARG = int, FUNCTION = public.shadow_op);
CREATE SCHEMA shadow;
CREATE FUNCTION public.review_helper() RETURNS text LANGUAGE sql AS $$ SELECT 'public' $$;
CREATE FUNCTION shadow.review_helper() RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$;
ALTER ROLE testuser SET search_path = shadow, public;
