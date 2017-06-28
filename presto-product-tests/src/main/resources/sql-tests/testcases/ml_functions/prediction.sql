-- database: presto; groups: ml_functions
--!
SELECT classify(features(1, 2 + random(1)), model)
FROM (
  SELECT learn_classifier(labels, features) AS model
  FROM (VALUES (1, features(1, 2))) t (labels, features)
) t2
--!
1|
