--
-- PostgreSQL database dump
--

-- Dumped from database version 14.17 (Homebrew)
-- Dumped by pg_dump version 14.17 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: franchise_scorigami; Type: TABLE; Schema: public; Owner: jaredconnolly
--

CREATE TABLE public.franchise_scorigami (
    team text,
    visitor_score bigint,
    home_score bigint,
    first_date text,
    first_park text,
    visitor_pitcher text,
    home_pitcher text
);


ALTER TABLE public.franchise_scorigami OWNER TO jaredconnolly;

--
-- Name: gamelogs; Type: TABLE; Schema: public; Owner: jaredconnolly
--

CREATE TABLE public.gamelogs (
    date text,
    visitor_team text,
    home_team text,
    visitor_score bigint,
    home_score bigint
);


ALTER TABLE public.gamelogs OWNER TO jaredconnolly;

--
-- Name: mlb_scorigami; Type: MATERIALIZED VIEW; Schema: public; Owner: jaredconnolly
--

CREATE MATERIALIZED VIEW public.mlb_scorigami AS
 SELECT gamelogs.home_score,
    gamelogs.visitor_score,
    gamelogs.home_team,
    gamelogs.visitor_team,
    count(*) AS n
   FROM public.gamelogs
  GROUP BY gamelogs.home_score, gamelogs.visitor_score, gamelogs.home_team, gamelogs.visitor_team
  WITH NO DATA;


ALTER TABLE public.mlb_scorigami OWNER TO jaredconnolly;

--
-- Name: teams; Type: TABLE; Schema: public; Owner: jaredconnolly
--

CREATE TABLE public.teams (
    team text,
    league text,
    city text,
    nickname text,
    first integer,
    last text,
    franchise text
);


ALTER TABLE public.teams OWNER TO jaredconnolly;

--
-- Name: teams_with_franchise; Type: TABLE; Schema: public; Owner: jaredconnolly
--

CREATE TABLE public.teams_with_franchise (
    "TEAM" text,
    "LEAGUE" text,
    "CITY" text,
    "NICKNAME" text,
    "FIRST" bigint,
    "LAST" bigint,
    "FRANCHISE" text
);


ALTER TABLE public.teams_with_franchise OWNER TO jaredconnolly;

--
-- Name: mlb_scorigami_home_team_idx; Type: INDEX; Schema: public; Owner: jaredconnolly
--

CREATE INDEX mlb_scorigami_home_team_idx ON public.mlb_scorigami USING btree (home_team);


--
-- Name: mlb_scorigami_visitor_team_idx; Type: INDEX; Schema: public; Owner: jaredconnolly
--

CREATE INDEX mlb_scorigami_visitor_team_idx ON public.mlb_scorigami USING btree (visitor_team);


--
-- PostgreSQL database dump complete
--

