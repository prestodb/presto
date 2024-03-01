-- database: presto; groups: color;
SELECT render('ala', color(0, rgb(255, 0, 0), rgb(0, 255, 0))), render('ala', color(0, 0, 10, rgb(255, 0, 0), rgb(0, 255, 0)))
