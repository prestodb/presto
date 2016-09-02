-- database: presto; groups: ml_connector
--!
SELECT classify(features(1, 2), model)
FROM (
  SELECT learn_classifier(labels, features) AS model
  FROM (VALUES (1, features(1, 2))) t (labels, features)
) t2
--!
1|
