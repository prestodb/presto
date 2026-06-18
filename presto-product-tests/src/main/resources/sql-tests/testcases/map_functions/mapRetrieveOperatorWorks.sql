-- database: presto; groups: qe, map_functions
select MAP(ARRAY ['ala', 'kot'], ARRAY[3, 4]) ['kot']
