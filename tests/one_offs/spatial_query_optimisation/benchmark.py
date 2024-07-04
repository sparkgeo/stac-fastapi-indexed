from json import dumps
from os import environ
from statistics import mean
from time import time
from typing import Dict, Final, List

from duckdb import connect as duckdb_connect
from mercantile import bounding_tile, quadkey
from shapely.geometry import Polygon
from shapely.wkt import loads as loads_wkt

items_s3_uri: Final[str] = environ["ITEMS_S3_URI"]
test_iterations: Final[int] = int(environ.get("TEST_ITERATIONS", 5))
query_geometry_wkts: Dict[str, str] = {
    "global": "POLYGON ((-180 -90,180 -90,180 90,-180 90,-180 -90))",
    "smithers": "POLYGON ((-127.238439419634 54.7546385434538,-127.216133194381 54.7549525045737,-127.219397520028 54.7891596724424,-127.197091294774 54.7951194300481,-127.153566952816 54.7973149087645,-127.138333433131 54.7850814373478,-127.127452347642 54.7665673544697,-127.131804781837 54.7502428321231,-127.151390735718 54.7433343215248,-127.169344526776 54.7329693457563,-127.207972380264 54.7335976015998,-127.207972380264 54.7335976015998,-127.238439419634 54.7546385434538))",
    "hudson bay": "POLYGON ((-89.7956040194495 64.0253370126454,-90.8846476700107 63.1042571102515,-94.1517786216942 61.484451988897,-94.5873960819186 59.6659184454069,-94.5873960819186 58.6044990856905,-92.9538306060769 58.6044990856905,-91.8647869555157 56.9795576883863,-90.1223171146178 57.5683047516667,-85.1127163220365 55.1562291562977,-81.845585370353 55.2804781296734,-81.7366810052969 52.7869628944168,-79.7764024342868 51.109067197905,-79.1229762439501 51.5852134698359,-79.1229762439501 54.5291481575514,-75.9647496573227 56.5618124321783,-76.8359845777717 57.6849269437784,-78.4695500536134 58.7177809649809,-76.8359845777717 60.3733451725328,-77.7072194982206 62.5572675408605,-89.7956040194495 64.0253370126454))",
    "greenland": "POLYGON ((-5.29324678376519 21.4412865854664,-5.42244380495602 21.1747640522225,-5.42244380495602 20.7870217264912,-5.33016021839114 20.4242275819838,-5.1363646866049 20.1039123969842,-4.95179751347515 19.8784326064386,-4.5457497325897 19.7308296054687,-3.95513477857449 19.7308296054687,-3.39220490052875 19.7916238922678,-3.10612578217763 19.8871108666228,-2.82927502248301 20.2425081810808,-2.42322724159755 20.5538960025572,-2.32171529637619 20.8646505711725,-2.20174663384185 21.1575524762313,-2.20174663384185 21.5614925863965,-2.19251827518536 21.9386325822581,-2.27557350309375 22.3062378127514,-2.52473918681891 22.4683613525924,-2.62625113204028 22.7409826315383,-3.04152727158222 22.9450927697373,-3.52140192171957 23.0300480688152,-4.02896164782639 22.9875770989978,-4.35195420080346 22.6899073214878,-4.37963927677292 22.391589547548,-4.34272584214697 22.1610201621018,-4.24121389692561 22.1439258517116,-4.08433179976532 22.2293766207598,-4.05664672379585 22.3659895168924,-4.02896164782639 22.4854159835097,-3.945906419918 22.5536134849577,-3.70596909484933 22.5536134849577,-3.40143325918524 22.5450906383623,-3.21686608605548 22.4427754696355,-2.86618845710896 22.2208338835951,-2.50628246950594 22.0156524469801,-2.50628246950594 21.6215582756255,-2.54319590413189 21.18336908933,-2.54319590413189 20.8474031752161,-2.7554481532311 20.7007206090556,-3.25377952068144 20.2251904488617,-3.53985863903255 20.0172276171295,-4.20430046229966 19.9825403101051,-4.57343480855916 20.086579273685,-4.96102587213164 20.2944497919974,-4.92411243750569 20.6489163363225,-4.66571839512403 20.5711768311011,-4.58266316721565 20.3809804425159,-4.34272584214697 20.3290678641725,-3.89053626797908 20.2857940646885,-3.63214222559743 20.3290678641725,-3.47526012843714 20.4588165450069,-3.207637727399 20.6057326192646,-3.04152727158222 20.8215283741627,-2.90310189173491 21.0542410399326,-2.90310189173491 21.2951889378662,-2.91233025039139 21.5614925863965,-3.24455116202495 21.6472930798324,-3.92744970260503 21.7930368702426,-5.08099453466598 21.8273080360609,-6.80669760342915 21.7330426095084,-8.39397529234502 21.7758982123899,-9.70440222156626 21.7244699545223,-10.9133172055661 21.8016054306746,-11.4577903662989 21.8444405414518,-12.2273929085604 21.897345048226,-6.46962574053002 20.9953302054284,-6.69765612342231 21.4470349442242,-5.29324678376519 21.4412865854664))",
    "mali": "POLYGON ((-44.3546782490988 60.8003475398014,-53.7546654320703 67.144291188305,-59.7364754575977 76.3217173243123,-68.2819183512081 76.1181724825346,-74.2637283767354 79.2061431332646,-62.3001083256807 79.9776922762723,-65.718285483125 79.9776922762723,-58.8819311682366 82.4425124531754,-48.6273996959041 82.4425124531754,-45.2092225384599 83.2904655591803,-26.4092481725169 83.583431339848,-17.8638052789065 82.664001867581,-35.8092353554884 82.664001867581,-34.9546910661274 81.9793961506054,-11.8819952533792 82.3292871423411,-11.8819952533792 81.1001238651292,-22.1365267257117 78.3759565672889,-17.0092609895455 75.9116590865713,-19.5728938576286 71.0924184476793,-34.9546910661274 66.4712689158678,-44.3546782490988 60.8003475398014))",
    "borneo": "POLYGON ((114.785728916331 1.77378594055605,113.708889065533 1.72313471933159,113.683551657279 1.49518812468421,113.518858503628 1.49518812468421,113.442846278866 1.73579765247222,113.100791267436 1.72313471933159,113.100791267436 1.48252369756905,113.430177574739 1.40653563511098,113.404840166485 1.31787977739093,112.91076070553 1.39387071384507,112.872754593149 1.08989359033783,113.062785155055 0.950559971119585,113.062785155055 0.798553275667121,113.227478308706 0.798553275667121,113.227478308706 0.735215432138943,112.936098113784 0.595869122568977,112.404012540449 0.215818229979136,112.163307162035 0.165143695162414,112.163307162035 0.000450770171062,112.289994203306 -0.01221793386337,112.289994203306 -0.240253904171095,112.606711806481 -0.202248075853023,112.78407366426 -0.100898810694286,112.682724031244 0.038456879664645,112.809411072514 0.000450770171062,113.100791267436 -0.01221793386337,113.100791267436 -0.062892737834052,112.78407366426 -0.100898810694286,112.961435522038 -0.227585305651794,113.316159237595 -0.227585305651794,113.949594443947 0.13980637683375,114.228305934742 0.228486838853619,114.228305934742 0.760550681705927,114.40566779252 0.671876690094693,114.380330384266 1.03922752220059,114.988428182364 1.10255997573937,115.381158010302 1.41920048763913,115.507845051572 1.21655494501796,115.837231358875 1.20388906690841,116.356648228084 1.34321035377307,116.534010085862 1.64715537406305,116.749378056022 1.65981880309207,116.825390280784 1.97637656369517,116.686034535386 2.06500233835057,116.635359718878 2.43211124510083,116.470666565227 2.64726754666225,116.318642115702 2.84973353009598,115.9892558084 2.8370804231663,115.9892558084 2.64726754666225,115.799225246494 2.64726754666225,115.913243583637 2.43211124510083,115.596525980462 2.06500233835057,115.317814489667 2.10298331249605,115.140452631888 1.65981880309207,114.785728916331 1.77378594055605))",
}
test_times: Dict[str, Dict[str, List[float]]] = {
    geom_name: {
        "standard": [],
        "minimum_bounding_quadkey_subquery": [],
        "minimum_bounding_quadkey_inline": [],
        "bbox_subquery": [],
        "bbox_inline": [],
    }
    for geom_name in query_geometry_wkts.keys()
}

db_connection = duckdb_connect()
db_connection.execute("INSTALL httpfs; LOAD httpfs")
db_connection.execute("INSTALL spatial; LOAD spatial")
db_connection.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN)")


def standard() -> Dict[str, List[str]]:
    test_name: Final[str] = "standard"
    result: Dict[str, List[str]] = {}
    for i in range(test_iterations):
        for geom_name, wkt in query_geometry_wkts.items():
            print(f"testing {test_name} {geom_name} ({i})")
            sql = """
                SELECT stac_location
                  FROM '{items_uri}'
                 WHERE ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))
              ORDER BY collection_id, id
                 LIMIT 10
            """.format(
                items_uri=items_s3_uri,
                wkt=wkt,
            )
            start = time()
            rows = db_connection.execute(sql).fetchall()
            duration = time() - start
            test_times[geom_name][test_name].append(duration)
            if i == 0:
                result[geom_name] = [row[0] for row in rows]
    return result


def minimum_bounding_quadkey_subquery(
    expected_stac_locations: Dict[str, List[str]],
) -> None:
    test_name: Final[str] = "minimum_bounding_quadkey_subquery"
    for i in range(test_iterations):
        for geom_name, wkt in query_geometry_wkts.items():
            print(f"testing {test_name} {geom_name} ({i})")
            query_geometry: Polygon = loads_wkt(wkt)
            query_minimum_bounding_quadkey = quadkey(
                bounding_tile(*query_geometry.bounds)
            )
            params = [query_minimum_bounding_quadkey]
            quadkey_ancestors_clause = ""
            if len(query_minimum_bounding_quadkey) > 0:
                query_minimum_bounding_quadkey_ancestors = [
                    "".join(query_minimum_bounding_quadkey[:i])
                    for i in range(len(query_minimum_bounding_quadkey))
                ]
                quadkey_ancestors_clause = "OR minimum_bounding_quadkey IN ({})".format(
                    ", ".join(
                        [
                            "?"
                            for _ in range(
                                len(query_minimum_bounding_quadkey_ancestors)
                            )
                        ]
                    )
                )
                params.extend(query_minimum_bounding_quadkey_ancestors)
            sql = """
                SELECT stac_location
                  FROM '{items_uri}'
                 WHERE unique_id IN (
                         SELECT unique_id
                           FROM '{items_uri}'
                          WHERE STARTS_WITH(minimum_bounding_quadkey, ?)
                             {quadkey_ancestors_clause}
                       )
                   AND ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))
              ORDER BY collection_id, id
                 LIMIT 10
            """.format(
                items_uri=items_s3_uri,
                quadkey_ancestors_clause=quadkey_ancestors_clause,
                wkt=wkt,
            )
            start = time()
            rows = db_connection.execute(sql, params).fetchall()
            duration = time() - start
            test_times[geom_name][test_name].append(duration)
            if i == 0:
                if not expected_stac_locations[geom_name] == [row[0] for row in rows]:
                    raise Exception(f"Incorrect result in {test_name} for {geom_name}")


def minimum_bounding_quadkey_inline(
    expected_stac_locations: Dict[str, List[str]],
) -> None:
    test_name: Final[str] = "minimum_bounding_quadkey_inline"
    for i in range(test_iterations):
        for geom_name, wkt in query_geometry_wkts.items():
            print(f"testing {test_name} {geom_name} ({i})")
            query_geometry: Polygon = loads_wkt(wkt)
            query_minimum_bounding_quadkey = quadkey(
                bounding_tile(*query_geometry.bounds)
            )
            params = [query_minimum_bounding_quadkey]
            quadkey_ancestors_clause = ""
            if len(query_minimum_bounding_quadkey) > 0:
                query_minimum_bounding_quadkey_ancestors = [
                    "".join(query_minimum_bounding_quadkey[:i])
                    for i in range(len(query_minimum_bounding_quadkey))
                ]
                quadkey_ancestors_clause = (
                    " OR minimum_bounding_quadkey IN ({})".format(
                        ", ".join(
                            [
                                "?"
                                for _ in range(
                                    len(query_minimum_bounding_quadkey_ancestors)
                                )
                            ]
                        )
                    )
                )
                params.extend(query_minimum_bounding_quadkey_ancestors)
            sql = """
                SELECT stac_location
                  FROM '{items_uri}'
                 WHERE (STARTS_WITH(minimum_bounding_quadkey, ?){quadkey_ancestors_clause})
                   AND ST_Intersects(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))
              ORDER BY collection_id, id
                 LIMIT 10
            """.format(
                items_uri=items_s3_uri,
                quadkey_ancestors_clause=quadkey_ancestors_clause,
                wkt=wkt,
            )
            start = time()
            rows = db_connection.execute(sql, params).fetchall()
            duration = time() - start
            test_times[geom_name][test_name].append(duration)
            if i == 0:
                if not expected_stac_locations[geom_name] == [row[0] for row in rows]:
                    raise Exception(f"Incorrect result in {test_name} for {geom_name}")


def bbox_subquery(expected_stac_locations: Dict[str, List[str]]) -> None:
    test_name: Final[str] = "bbox_subquery"
    for i in range(test_iterations):
        for geom_name, wkt in query_geometry_wkts.items():
            print(f"testing {test_name} {geom_name} ({i})")
            query_geometry: Polygon = loads_wkt(wkt)
            sql = """
                SELECT stac_location
                  FROM '{items_uri}'
                WHERE unique_id IN (
                        SELECT unique_id
                          FROM '{items_uri}'
                         WHERE NOT (
                                 bbox_x_max < ?  -- query x min
                              OR bbox_y_max < ?  -- query y min
                              OR bbox_x_min > ?  -- query x max
                              OR bbox_y_min > ?  -- query y max
                               )
                      )
                  AND ST_Intersects(
                        ST_GeomFromWKB(geometry),
                        ST_GeomFromText('{wkt}')
                      )
             ORDER BY collection_id, id
                LIMIT 10
            """.format(
                items_uri=items_s3_uri,
                wkt=wkt,
            )
            params = query_geometry.bounds
            start = time()
            rows = db_connection.execute(sql, params).fetchall()
            duration = time() - start
            test_times[geom_name][test_name].append(duration)
            if i == 0:
                if not expected_stac_locations[geom_name] == [row[0] for row in rows]:
                    raise Exception(f"Incorrect result in {test_name} for {geom_name}")


def bbox_inline(expected_stac_locations: Dict[str, List[str]]) -> None:
    test_name: Final[str] = "bbox_inline"
    for i in range(test_iterations):
        for geom_name, wkt in query_geometry_wkts.items():
            print(f"testing {test_name} {geom_name} ({i})")
            query_geometry: Polygon = loads_wkt(wkt)
            sql = """
                SELECT stac_location
                  FROM '{items_uri}'
                WHERE NOT (
                        bbox_x_max < ?  -- query x min
                     OR bbox_y_max < ?  -- query y min
                     OR bbox_x_min > ?  -- query x max
                     OR bbox_y_min > ?  -- query y max
                      )
                  AND ST_Intersects(
                        ST_GeomFromWKB(geometry),
                        ST_GeomFromText('{wkt}')
                      )
             ORDER BY collection_id, id
                LIMIT 10
            """.format(
                items_uri=items_s3_uri,
                wkt=wkt,
            )
            params = query_geometry.bounds
            start = time()
            rows = db_connection.execute(sql, params).fetchall()
            duration = time() - start
            test_times[geom_name][test_name].append(duration)
            if i == 0:
                if not expected_stac_locations[geom_name] == [row[0] for row in rows]:
                    raise Exception(f"Incorrect result in {test_name} for {geom_name}")


expected_stac_locations = standard()
minimum_bounding_quadkey_subquery(expected_stac_locations)
minimum_bounding_quadkey_inline(expected_stac_locations)
bbox_subquery(expected_stac_locations)
bbox_inline(expected_stac_locations)

report: Dict[str, Dict[str, Dict[str, float]]] = {}
for geom_name, tests in test_times.items():
    report[geom_name] = {}
    for test_name, values in tests.items():
        report[geom_name][test_name] = {
            "mean": mean(values),
            "min": min(values),
            "max": max(values),
        }

print("-----")
print(dumps(report, indent=2))
print("-----")
