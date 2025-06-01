psql -U jaredconnolly -d mlb_scorigami \
-c "\copy (
        SELECT  g.home_score,
                g.visitor_score,
                COUNT(*) AS occurrences
        FROM    gamelogs g
        JOIN    teams    th ON th.team = g.home_team
        JOIN    teams    tv ON tv.team = g.visitor_team
        WHERE   COALESCE(th.franchise, '') <> ''
          AND   COALESCE(tv.franchise, '') <> ''
        GROUP BY g.home_score, g.visitor_score
        ORDER BY g.home_score, g.visitor_score
      )
      TO 'outputs/mlb_scorigami_scores_franchise.csv' CSV HEADER"