/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <geos/geom/Coordinate.h>
#include <geos/geom/Envelope.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKBWriter.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/simplify/TopologyPreservingSimplifier.h>
#include <geos/util/AssertionFailedException.h>
#include <geos/util/UnsupportedOperationException.h>
#include <cmath>

#include <velox/type/StringView.h>
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/geospatial/GeometrySerde.h"
#include "velox/functions/prestosql/geospatial/GeometryUtils.h"
#include "velox/functions/prestosql/types/GeometryType.h"

namespace facebook::velox::functions {

// Constructors and Serde

template <typename T>
struct StGeometryFromTextFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Geometry>& result, const arg_type<Varchar>& wkt) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry;
    GEOS_TRY(
        {
          geos::io::WKTReader reader;
          geosGeometry = reader.read(wkt);
        },
        "Failed to parse WKT");
    result = geospatial::serializeGeometry(*geosGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StGeomFromBinaryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Geometry>& result, const arg_type<Varbinary>& wkb) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry;

    GEOS_TRY(
        {
          geos::io::WKBReader reader;
          geosGeometry = reader.read(
              reinterpret_cast<const uint8_t*>(wkb.data()), wkb.size());
        },
        "Failed to parse WKB");
    result = geospatial::serializeGeometry(*geosGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StAsTextFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Varchar>& result, const arg_type<Geometry>& geometry) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(geometry);

    GEOS_TRY(
        {
          geos::io::WKTWriter writer;
          writer.setTrim(true);
          result = writer.write(geosGeometry.get());
        },
        "Failed to write WKT");
    return Status::OK();
  }
};

template <typename T>
struct StAsBinaryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Varbinary>& result, const arg_type<Geometry>& geometry) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(geometry);
    GEOS_TRY(
        {
          geos::io::WKBWriter writer;
          std::ostringstream outputStream;
          writer.write(*geosGeometry, outputStream);
          result = outputStream.str();
        },
        "Failed to write WKB");
    return Status::OK();
  }
};

template <typename T>
struct StPointFunction {
  StPointFunction() {
    factory_ = geos::geom::GeometryFactory::create();
  }

  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<double>& x,
      const arg_type<double>& y) {
    if (!std::isfinite(x) || !std::isfinite(y)) {
      return Status::UserError(fmt::format(
          "ST_Point requires finite coordinates, got x={} y={}", x, y));
    }
    GEOS_TRY(
        {
          auto point = std::unique_ptr<geos::geom::Point>(
              factory_->createPoint(geos::geom::Coordinate(x, y)));
          result = geospatial::serializeGeometry(*point);
        },
        "Failed to create point geometry");
    return Status::OK();
  }

 private:
  geos::geom::GeometryFactory::Ptr factory_;
};

// Predicates

template <typename T>
struct StRelateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry,
      const arg_type<Varchar>& relation) {
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->relate(*rightGeosGeometry, relation);
             , "Failed to check geometry relation");

    return Status::OK();
  }
};

template <typename T>
struct StContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->contains(&*rightGeosGeometry);
             , "Failed to check geometry contains");

    return Status::OK();
  }
};

template <typename T>
struct StCrossesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->crosses(&*rightGeosGeometry);
             , "Failed to check geometry crosses");

    return Status::OK();
  }
};

template <typename T>
struct StDisjointFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->disjoint(&*rightGeosGeometry);
             , "Failed to check geometry disjoint");

    return Status::OK();
  }
};

template <typename T>
struct StEqualsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->equals(&*rightGeosGeometry);
             , "Failed to check geometry equals");

    return Status::OK();
  }
};

template <typename T>
struct StIntersectsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->intersects(&*rightGeosGeometry);
             , "Failed to check geometry intersects");

    return Status::OK();
  }
};

template <typename T>
struct StOverlapsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->overlaps(&*rightGeosGeometry);
             , "Failed to check geometry overlaps");

    return Status::OK();
  }
};

template <typename T>
struct StTouchesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->touches(&*rightGeosGeometry);
             , "Failed to check geometry touches");

    return Status::OK();
  }
};

template <typename T>
struct StWithinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<bool>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);
    GEOS_TRY(result = leftGeosGeometry->within(&*rightGeosGeometry);
             , "Failed to check geometry within");

    return Status::OK();
  }
};

// Overlay operations

template <typename T>
struct StDifferenceFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    // if envelopes are disjoint
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;
    GEOS_TRY(outputGeometry = leftGeosGeometry->difference(&*rightGeosGeometry);
             , "Failed to compute geometry difference");

    result = geospatial::serializeGeometry(*outputGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StBoundaryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Geometry>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;

    GEOS_TRY(
        result = geospatial::serializeGeometry(*geosGeometry->getBoundary());
        , "Failed to compute geometry boundary");

    return Status::OK();
  }
};

template <typename T>
struct StIntersectionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    // if envelopes are disjoint
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;
    GEOS_TRY(
        outputGeometry = leftGeosGeometry->intersection(&*rightGeosGeometry);
        , "Failed to compute geometry intersection");

    result = geospatial::serializeGeometry(*outputGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StSymDifferenceFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit
    // if envelopes are disjoint
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;
    GEOS_TRY(
        outputGeometry = leftGeosGeometry->symDifference(&*rightGeosGeometry);
        , "Failed to compute geometry symdifference");

    result = geospatial::serializeGeometry(*outputGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StUnionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<Geometry>& leftGeometry,
      const arg_type<Geometry>& rightGeometry) {
    // TODO: When #12771 is merged, check envelopes and short-circuit if
    // one/both are empty
    std::unique_ptr<geos::geom::Geometry> leftGeosGeometry =
        geospatial::deserializeGeometry(leftGeometry);
    std::unique_ptr<geos::geom::Geometry> rightGeosGeometry =
        geospatial::deserializeGeometry(rightGeometry);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;
    GEOS_TRY(outputGeometry = leftGeosGeometry->Union(&*rightGeosGeometry);
             , "Failed to compute geometry union");

    result = geospatial::serializeGeometry(*outputGeometry);
    return Status::OK();
  }
};

// Accessors

template <typename T>
struct StIsValidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<bool>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    GEOS_TRY(result = geosGeometry->isValid();
             , "Failed to check geometry isValid");

    return Status::OK();
  }
};

template <typename T>
struct StIsSimpleFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<bool>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    GEOS_TRY(result = geosGeometry->isSimple();
             , "Failed to check geometry isSimple");

    return Status::OK();
  }
};

template <typename T>
struct GeometryInvalidReasonFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    std::optional<std::string> messageOpt =
        geospatial::geometryInvalidReason(geosGeometry.get());

    if (messageOpt.has_value()) {
      result = messageOpt.value();
    }
    return messageOpt.has_value();
  }
};

template <typename T>
struct StAreaFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<double>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    std::unique_ptr<geos::geom::Geometry> outputGeometry;

    GEOS_TRY(result = geosGeometry->getArea();
             , "Failed to compute geometry area");

    return Status::OK();
  }
};

template <typename T>
struct StCentroidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Geometry>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    auto validate = facebook::velox::functions::geospatial::validateType(
        *geosGeometry,
        {geos::geom::GeometryTypeId::GEOS_POINT,
         geos::geom::GeometryTypeId::GEOS_MULTIPOINT,
         geos::geom::GeometryTypeId::GEOS_LINESTRING,
         geos::geom::GeometryTypeId::GEOS_MULTILINESTRING,
         geos::geom::GeometryTypeId::GEOS_POLYGON,
         geos::geom::GeometryTypeId::GEOS_MULTIPOLYGON},
        "ST_Centroid");

    if (!validate.ok()) {
      return validate;
    }

    geos::geom::GeometryTypeId type = geosGeometry->getGeometryTypeId();
    if (type == geos::geom::GeometryTypeId::GEOS_POINT) {
      result = input;
      return Status::OK();
    }

    if (geosGeometry->getNumPoints() == 0) {
      GEOS_TRY(
          {
            geos::geom::GeometryFactory::Ptr factory =
                geos::geom::GeometryFactory::create();
            std::unique_ptr<geos::geom::Point> point = factory->createPoint();
            result = geospatial::serializeGeometry(*point);
            factory->destroyGeometry(point.release());
          },
          "Failed to create point geometry");
      return Status::OK();
    }

    result = geospatial::serializeGeometry(*(geosGeometry->getCentroid()));
    return Status::OK();
  }
};

template <typename T>
struct StXFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<Geometry>& geometry) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(geometry);
    if (geosGeometry->getGeometryTypeId() !=
        geos::geom::GeometryTypeId::GEOS_POINT) {
      VELOX_USER_FAIL(fmt::format(
          "ST_X requires a Point geometry, found {}",
          geosGeometry->getGeometryType()));
    }
    if (geosGeometry->isEmpty()) {
      return false;
    }
    auto coordinate = geosGeometry->getCoordinate();
    result = coordinate->x;
    return true;
  }
};

template <typename T>
struct StYFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<Geometry>& geometry) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(geometry);
    if (geosGeometry->getGeometryTypeId() !=
        geos::geom::GeometryTypeId::GEOS_POINT) {
      VELOX_USER_FAIL(fmt::format(
          "ST_Y requires a Point geometry, found {}",
          geosGeometry->getGeometryType()));
    }
    if (geosGeometry->isEmpty()) {
      return false;
    }
    auto coordinate = geosGeometry->getCoordinate();
    result = coordinate->y;
    return true;
  }
};

template <typename T>
struct StXMinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(out_type<double>& result, const arg_type<Geometry>& geometry) {
    const std::unique_ptr<geos::geom::Envelope> env =
        geospatial::getEnvelopeFromGeometry(geometry);
    if (env->isNull()) {
      return false;
    }
    result = env->getMinX();
    return true;
  }
};

template <typename T>
struct StYMinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(out_type<double>& result, const arg_type<Geometry>& geometry) {
    const std::unique_ptr<geos::geom::Envelope> env =
        geospatial::getEnvelopeFromGeometry(geometry);
    if (env->isNull()) {
      return false;
    }
    result = env->getMinY();
    return true;
  }
};

template <typename T>
struct StXMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(out_type<double>& result, const arg_type<Geometry>& geometry) {
    const std::unique_ptr<geos::geom::Envelope> env =
        geospatial::getEnvelopeFromGeometry(geometry);
    if (env->isNull()) {
      return false;
    }
    result = env->getMaxX();
    return true;
  }
};

template <typename T>
struct StYMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(out_type<double>& result, const arg_type<Geometry>& geometry) {
    const std::unique_ptr<geos::geom::Envelope> env =
        geospatial::getEnvelopeFromGeometry(geometry);
    if (env->isNull()) {
      return false;
    }
    result = env->getMaxY();
    return true;
  }
};

template <typename T>
struct SimplifyGeometryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status call(
      out_type<Geometry>& result,
      const arg_type<Geometry>& geometry,
      const arg_type<double>& tolerance) {
    if (!std::isfinite(tolerance) || tolerance < 0) {
      return Status::UserError(
          "simplification tolerance must be a non-negative finite number");
    }
    if (tolerance == 0) {
      result = geometry;
      return Status::OK();
    }

    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(geometry);

    if (geosGeometry->isEmpty()) {
      result = geometry;
      return Status::OK();
    }

    std::unique_ptr<geos::geom::Geometry> outputGeometry;
    GEOS_TRY(
        {
          outputGeometry =
              geos::simplify::TopologyPreservingSimplifier::simplify(
                  geosGeometry.get(), tolerance);
        },
        "Failed to compute simplified geometry");

    result = geospatial::serializeGeometry(*outputGeometry);
    return Status::OK();
  }
};

template <typename T>
struct StGeometryTypeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE Status
  call(out_type<Varchar>& result, const arg_type<Geometry>& input) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry =
        geospatial::deserializeGeometry(input);

    result = geosGeometry->getGeometryType();

    return Status::OK();
  }
};

template <typename T>
struct StDistanceFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<double>& result,
      const arg_type<Geometry>& geometry1,
      const arg_type<Geometry>& geometry2) {
    std::unique_ptr<geos::geom::Geometry> geosGeometry1 =
        geospatial::deserializeGeometry(geometry1);
    std::unique_ptr<geos::geom::Geometry> geosGeometry2 =
        geospatial::deserializeGeometry(geometry2);

    if (geosGeometry1->getSRID() != geosGeometry2->getSRID()) {
      VELOX_USER_FAIL(fmt::format(
          "Input geometries must have the same spatial reference, found {} and {}",
          geosGeometry1->getSRID(),
          geosGeometry2->getSRID()));
    }

    if (geosGeometry1->isEmpty() || geosGeometry2->isEmpty()) {
      return false;
    }

    GEOS_RETHROW(result = geosGeometry1->distance(geosGeometry2.get());
                 , "Failed to calculate geometry distance");

    return true;
  }
};

} // namespace facebook::velox::functions
