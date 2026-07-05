// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.compat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.impala.hive.geospatial.esri.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.impala.builtins.ST_ConvexHull_Wrapper;
import org.apache.impala.builtins.ST_LineString_Wrapper;
import org.apache.impala.builtins.ST_MultiPoint_Wrapper;
import org.apache.impala.builtins.ST_Polygon_Wrapper;
import org.apache.impala.builtins.ST_Union_Wrapper;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.hive.executor.BinaryToBinaryHiveLegacyFunctionExtractor;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveLegacyFunctionExtractor;
import org.apache.impala.hive.executor.HiveLegacyJavaFunction;
import org.apache.impala.hive.executor.JavaUdfDataType;
import org.apache.impala.service.BackendConfig;

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.FunctionName;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TGeospatialLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveEsriGeospatialBuiltins {
  private final static Logger LOG = LoggerFactory.getLogger(
      HiveEsriGeospatialBuiltins.class);

  /**
   * Extractor used in WKB_EXPERIMENTAL mode: maps the legacy ESRI UDFs' BytesWritable
   * parameters/return values to GEOMETRY instead of BINARY.
   */
  private static class BinaryToGeometryHiveLegacyFunctionExtractor
      extends HiveLegacyFunctionExtractor {
    @Override
    protected ScalarType resolveType(
        Class<?> type, java.util.function.Function<JavaUdfDataType, String> errorHandler)
        throws ImpalaException {
      if (type == BytesWritable.class) {
        return ScalarType.createType(PrimitiveType.GEOMETRY);
      } else {
        return super.resolveType(type, errorHandler);
      }
    }
  }

  /**
   * Initializes Hive's ESRI geospatial UDFs as builtins.
   */
  public static void initBuiltins(Db db) {
    TGeospatialLibrary lib = BackendConfig.INSTANCE.getGeospatialLibrary();

    // Set the serialization format on both branches so it is self-correcting
    // even if initBuiltins were ever re-run with a different library.
    GeometryUtils.setFormat(lib.equals(TGeospatialLibrary.WKB_EXPERIMENTAL)
        ? GeometryUtils.SerializationFormat.WKB
        : GeometryUtils.SerializationFormat.ESRI_SHAPE);

    boolean addNatives = lib.equals(TGeospatialLibrary.HIVE_ESRI);
    boolean isWkb = lib.equals(TGeospatialLibrary.WKB_EXPERIMENTAL);
    addLegacyUDFs(db, addNatives, isWkb);
    addGenericUDFs(db, isWkb);
    addVarargsUDFs(db, isWkb);
    if (addNatives) {
      addNatives(db);
    }
    if (isWkb) {
      addWkbNatives(db);
    }
  }

  private static void addLegacyUDFs(Db db, boolean addNatives, boolean isWkb) {
    // Functions that take raw bytes and produce geometry (BINARY -> GEOMETRY in WKB
    // mode).
    List<UDF> binaryInputConstructors = Arrays.asList(
        new ST_GeomFromWKB(), new ST_GeomFromShape(),
        new ST_PointFromWKB(), new ST_LineFromWKB(),
        new ST_MLineFromWKB(), new ST_MPointFromWKB(),
        new ST_MPolyFromWKB(), new ST_PolyFromWKB());

    // Functions that serialize geometry to raw bytes (GEOMETRY -> BINARY in WKB mode).
    List<UDF> serializers = Arrays.asList(new ST_AsBinary(), new ST_AsShape());

    // Functions that always run as Java (no C++ native in either mode).
    List<UDF> legacyUDFs = new ArrayList<>(Arrays.asList(
        new ST_AsGeoJson(), new ST_AsJson(),
        new ST_Boundary(), new ST_Buffer(), new ST_CoordDim(),
        new ST_Difference(), new ST_DistanceSphere(),
        new ST_GeodesicLengthWGS84(), new ST_GeomCollection(), new ST_GeometryN(),
        new ST_Intersection(),
        new ST_Is3D(), new ST_IsMeasured(),
        new ST_M(), new ST_MaxM(), new ST_MaxZ(),
        new ST_MinM(), new ST_MinZ(),
        new ST_Relate(), new ST_SymmetricDiff(),
        new ST_Z()));

    // Functions with native C++ implementations in HIVE_ESRI mode (ESRI Shape format).
    List<UDF> legacyUDFsWithEsriNative = Arrays.asList(
        new ST_EnvIntersects(), new ST_GeometryType(),
        new ST_MaxX(), new ST_MaxY(),
        new ST_MinX(), new ST_MinY(),
        new ST_SRID(), new ST_SetSRID(),
        new ST_X(), new ST_Y()
    );

    // Functions with native C++ implementations in WKB_EXPERIMENTAL mode.
    List<UDF> legacyUDFsWithWkbNative = Arrays.asList(
        new ST_Area(), new ST_AsText(), new ST_Centroid(),
        new ST_Dimension(), new ST_Distance(), new ST_EndPoint(),
        new ST_Envelope(), new ST_ExteriorRing(), new ST_GeomFromText(),
        new ST_InteriorRingN(), new ST_IsClosed(), new ST_IsEmpty(),
        new ST_IsRing(), new ST_IsSimple(), new ST_Length(),
        new ST_NumGeometries(), new ST_NumInteriorRing(), new ST_NumPoints(),
        new ST_PointN(), new ST_StartPoint()
    );

    if (addNatives) {
      // HIVE_ESRI: ESRI natives handle legacyUDFsWithEsriNative, rest stays Java.
      legacyUDFs.addAll(legacyUDFsWithWkbNative);
    } else if (isWkb) {
      // WKB_EXPERIMENTAL: WKB natives handle both lists. Add neither.
    } else {
      // No natives: all run as Java.
      legacyUDFs.addAll(legacyUDFsWithEsriNative);
      legacyUDFs.addAll(legacyUDFsWithWkbNative);
    }

    HiveLegacyFunctionExtractor extractor = isWkb
        ? new BinaryToGeometryHiveLegacyFunctionExtractor()
        : new BinaryToBinaryHiveLegacyFunctionExtractor();

    for (UDF udf : legacyUDFs) {
      for (Function fn : extractFromLegacyHiveBuiltin(udf, db.getName(), extractor)) {
        db.addBuiltin(fn);
      }
    }

    if (isWkb) {
      // Binary-input constructors: BINARY param, GEOMETRY return.
      for (UDF udf : binaryInputConstructors) {
        String fnName = udf.getClass().getSimpleName().toLowerCase();
        db.addBuiltin(
            createScalarFunction(udf.getClass(), fnName, Type.GEOMETRY,
                new Type[]{Type.BINARY}));
      }
      // Serializers: GEOMETRY param, BINARY return.
      for (UDF udf : serializers) {
        String fnName = udf.getClass().getSimpleName().toLowerCase();
        db.addBuiltin(
            createScalarFunction(udf.getClass(), fnName, Type.BINARY,
                new Type[]{Type.GEOMETRY}));
      }
    } else {
      // In non-WKB mode, register all with the standard extractor.
      for (UDF udf : binaryInputConstructors) {
        for (Function fn : extractFromLegacyHiveBuiltin(udf, db.getName(), extractor)) {
          db.addBuiltin(fn);
        }
      }
      for (UDF udf : serializers) {
        for (Function fn : extractFromLegacyHiveBuiltin(udf, db.getName(), extractor)) {
          db.addBuiltin(fn);
        }
      }
    }

    // st_point is a special case as it has both Java and native overloads
    addJavaStPoint(db, addNatives, isWkb);
  }

  private static void addGenericUDFs(Db db, boolean isWkb) {
    List<ScalarFunction> genericUDFs = new ArrayList<>();
    Type geomType = isWkb ? Type.GEOMETRY : Type.BINARY;

    if (!isWkb) {
      // In WKB mode, ST_Bin and ST_BinEnvelope have C++ native implementations.
      List<Set<Type>> stBinArguments =
          ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
              ImmutableSet.of(Type.STRING, geomType));
      List<Set<Type>> stBinEnvelopeArguments =
          ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
              ImmutableSet.of(Type.STRING, geomType, Type.BIGINT));

      genericUDFs.addAll(
          createMappedGenericUDFs(stBinArguments, Type.BIGINT, ST_Bin.class));
      genericUDFs.addAll(createMappedGenericUDFs(
          stBinEnvelopeArguments, geomType, ST_BinEnvelope.class));
    }

    genericUDFs.add(createScalarFunction(
        ST_GeomFromGeoJson.class, geomType, new Type[] {Type.STRING}));
    genericUDFs.add(createScalarFunction(
        ST_GeomFromJson.class, geomType, new Type[] {Type.STRING}));

    if (!isWkb) {
      // In WKB mode, ST_MultiPolygon and ST_MultiLineString have C++ natives.
      genericUDFs.add(createScalarFunction(
          ST_MultiPolygon.class, geomType, new Type[] {Type.STRING}));
      genericUDFs.add(createScalarFunction(
          ST_MultiLineString.class, geomType, new Type[] {Type.STRING}));
    }

    if (!isWkb) {
      // In WKB mode, relational predicates have C++ native implementations.
      createRelationalGenericUDFs(genericUDFs, isWkb);
    }

    for (ScalarFunction function : genericUDFs) {
      db.addBuiltin(function);
    }
  }

  private static void createRelationalGenericUDFs(
      List<ScalarFunction> genericUDFs, boolean isWkb) {
    List<GenericUDF> relationalUDFs = Arrays.asList(new ST_Contains(), new ST_Crosses(),
        new ST_Disjoint(), new ST_Equals(), new ST_Intersects(), new ST_Overlaps(),
        new ST_Touches(), new ST_Within());

    List<Set<Type>> relationalUDFArguments =
        ImmutableList.of(ImmutableSet.of(Type.STRING, Type.BINARY),
            ImmutableSet.of(Type.STRING, Type.BINARY));

    for (GenericUDF relationalUDF : relationalUDFs) {
      genericUDFs.addAll(createMappedGenericUDFs(
          relationalUDFArguments, Type.BOOLEAN, relationalUDF.getClass()));
    }
  }

  private static void addVarargsUDFs(Db db, boolean isWkb) {
    HiveLegacyFunctionExtractor extractor = isWkb
        ? new BinaryToGeometryHiveLegacyFunctionExtractor()
        : new BinaryToBinaryHiveLegacyFunctionExtractor();

    List<ScalarFunction> varargsUDFs = new ArrayList<>();
    varargsUDFs.addAll(
        extractFunctions(ST_Union_Wrapper.class, ST_Union.class, db.getName(),
            extractor));
    if (!isWkb) {
      // In WKB mode, ST_Polygon, ST_LineString, and ST_MultiPoint have C++ natives.
      varargsUDFs.addAll(
          extractFunctions(ST_Polygon_Wrapper.class, ST_Polygon.class, db.getName(),
              extractor));
      varargsUDFs.addAll(
          extractFunctions(ST_LineString_Wrapper.class, ST_LineString.class, db.getName(),
              extractor));
      varargsUDFs.addAll(
          extractFunctions(ST_MultiPoint_Wrapper.class, ST_MultiPoint.class, db.getName(),
              extractor));
    }
    varargsUDFs.addAll(
        extractFunctions(ST_ConvexHull_Wrapper.class, ST_ConvexHull.class, db.getName(),
            extractor));

    for (ScalarFunction function : varargsUDFs) {
      db.addBuiltin(function);
    }
  }

  private static void addJavaStPoint(Db db, boolean addNatives, boolean isWkb) {
    // Create only specific overloads for st_point and st_pointz.
    // Unlike Hive, overloads with more dimensions are not added to avoid conflict with
    // PostGis's optional "integer srid=unknown" argument, which is implicitly castable
    // from double. See HIVE-29395 for details.
    // There is no ST_PointM and ST_PointZM at the moment so points with M dimension can
    // be created only with the WKT constructor.
    Type geomType = isWkb ? Type.GEOMETRY : Type.BINARY;
    if (!addNatives && !isWkb) {
      // In HIVE_ESRI mode, st_point(DOUBLE,DOUBLE) is native C++.
      // In WKB mode, both st_point(DOUBLE,DOUBLE) and st_point(STRING) are native C++.
      Type[] args2d = {Type.DOUBLE, Type.DOUBLE};
      db.addBuiltin(
          createScalarFunction(ST_Point.class, "st_point", geomType, args2d));
    }
    Type[] args3d = {Type.DOUBLE, Type.DOUBLE, Type.DOUBLE};
    db.addBuiltin(
        createScalarFunction(ST_PointZ.class, "st_pointz", geomType, args3d));
    if (!isWkb) {
      // In WKB mode, st_point(STRING) is handled by C++ native st_Point_WKB(StringVal).
      Type[] argsWkt = {Type.STRING};
      db.addBuiltin(
          createScalarFunction(ST_Point.class, "st_point", geomType, argsWkt));
    }
  }

  private static List<ScalarFunction> extractFromLegacyHiveBuiltin(
      UDF udf, String dbName, HiveLegacyFunctionExtractor extractor) {
    return extractFunctions(udf.getClass(), udf.getClass(), dbName, extractor);
  }

  private static List<ScalarFunction> extractFunctions(
      Class<?> udfClass, Class<?> signatureClass, String dbName,
      HiveLegacyFunctionExtractor extractor) {
    // The function has the same name as the signature class name
    String fnName = signatureClass.getSimpleName().toLowerCase();
    // The symbol name is coming from the UDF class which contains the functions
    String symbolName = udfClass.getName();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
        HiveJavaFunction.createHiveFunction(fnName, dbName, symbolName, null);
    try {
      return new HiveLegacyJavaFunction(udfClass, hiveFunction, null, null)
          .extract(extractor);
    } catch (CatalogException ex) {
      // It is a fatal error if we fail to load a builtin function.
      Preconditions.checkState(false, ex.getMessage());
      return Collections.emptyList();
    }
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, String name, Type returnType, Type[] arguments) {
    ScalarFunction function = new ScalarFunction(
        new FunctionName(BuiltinsDb.NAME, name), arguments, returnType, false);
    function.setSymbolName(udf.getName());
    function.setUserVisible(true);
    function.setHasVarArgs(false);
    function.setBinaryType(TFunctionBinaryType.JAVA);
    function.setIsPersistent(true);
    return function;
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, Type returnType, Type[] arguments) {
    return createScalarFunction(
        udf, udf.getSimpleName().toLowerCase(), returnType, arguments);
  }

  private static List<ScalarFunction> createMappedGenericUDFs(
      List<Set<Type>> listOfArgumentOptions, Type returnType, Class<?> genericUDF) {
    return Sets.cartesianProduct(listOfArgumentOptions)
        .stream()
        .map(types -> {
          Type[] arguments = types.toArray(new Type[0]);
          return createScalarFunction(genericUDF, returnType, arguments);
        })
        .collect(Collectors.toList());
  }

  private static final String GEO_FN_PREFIX =
      "impala::geo::GeospatialFunctions::";
  private static final String GEOM_WRAPPER_PREPARE =
      GEO_FN_PREFIX + "GeometryWrapperPrepare";
  private static final String GEOM_WRAPPER_CLOSE =
      GEO_FN_PREFIX + "GeometryWrapperClose";
  private static final String RELATION_WRAPPER_PREPARE =
      GEO_FN_PREFIX + "RelationWrapperPrepare";
  private static final String RELATION_WRAPPER_CLOSE =
      GEO_FN_PREFIX + "RelationWrapperClose";

  private static void addNative(Db db, String fnNameBase, String fnNameSuffix,
      boolean varArgs, Type retType, Type... argTypes) {
    String udfName = fnNameBase.toLowerCase();
    String cppSymbolName = GEO_FN_PREFIX + fnNameBase + fnNameSuffix;
    db.addScalarBuiltin(udfName, cppSymbolName, true, varArgs, retType, argTypes);
  }

  private static void addNative(Db db, String fnNameBase, String fnNameSuffix,
      boolean varArgs, Type retType, String prepareFn, String closeFn,
      Type... argTypes) {
    String udfName = fnNameBase.toLowerCase();
    String cppSymbolName = GEO_FN_PREFIX + fnNameBase + fnNameSuffix;
    db.addScalarBuiltin(udfName, cppSymbolName, true, prepareFn, closeFn,
        varArgs, retType, argTypes);
  }

  private static void addNativeWithGeomState(Db db, String fnNameBase,
      String fnNameSuffix, boolean varArgs, Type retType, Type... argTypes) {
    addNative(db, fnNameBase, fnNameSuffix, varArgs, retType,
        GEOM_WRAPPER_PREPARE, GEOM_WRAPPER_CLOSE, argTypes);
  }

  private static void addNativeWithRelationState(Db db, String fnNameBase,
      String fnNameSuffix, boolean varArgs, Type retType, Type... argTypes) {
    addNative(db, fnNameBase, fnNameSuffix, varArgs, retType,
        RELATION_WRAPPER_PREPARE, RELATION_WRAPPER_CLOSE, argTypes);
  }

  private static void addNative(Db db, String fnName, boolean varArgs, Type retType,
      Type... argTypes) {
    addNative(db, fnName, "", varArgs, retType, argTypes);
  }

  private static void addNatives(Db db) {
    // Legacy UDFs.

    // Accessors.
    addNative(db, "st_MinX", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_MaxX", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_MinY", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_MaxY", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_X", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_Y", false, Type.DOUBLE, Type.BINARY);
    addNative(db, "st_Srid", false, Type.INT, Type.BINARY);
    addNative(db, "st_SetSrid", false, Type.BINARY, Type.BINARY, Type.INT);
    addNative(db, "st_GeometryType", false, Type.STRING, Type.BINARY);

    // Constructors.
    // Other point constructors are added as Java UDFs, see addJavaStPoint().
    addNative(db, "st_Point", false, Type.BINARY, Type.DOUBLE, Type.DOUBLE);

    // Predicates.
    addNative(db, "st_EnvIntersects", false, Type.BOOLEAN, Type.BINARY, Type.BINARY);
  }

  private static void addWkbNatives(Db db) {
    // Accessors.
    addNative(db, "st_X", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_Y", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_MinX", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_MaxX", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_MinY", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_MaxY", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_GeometryType", "_WKB", false, Type.STRING, Type.GEOMETRY);
    addNative(db, "st_Srid", "_WKB", false, Type.INT, Type.GEOMETRY);
    addNative(db, "st_SetSrid", "_WKB", false, Type.GEOMETRY, Type.GEOMETRY, Type.INT);

    // Constructors.
    addNative(db, "st_Point", "_WKB", false, Type.GEOMETRY, Type.DOUBLE, Type.DOUBLE);
    addNativeWithGeomState(db, "st_Point", "_WKB", false, Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_LineString", "_WKB", false, Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_LineString", "_WKB", true, Type.GEOMETRY, Type.DOUBLE);
    addNativeWithGeomState(db, "st_MultiPoint", "_WKB", false, Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_MultiPoint", "_WKB", true, Type.GEOMETRY, Type.DOUBLE);
    addNativeWithGeomState(db, "st_Polygon", "_WKB", false, Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_Polygon", "_WKB", true, Type.GEOMETRY, Type.DOUBLE);
    addNativeWithGeomState(db, "st_MultiLineString", "_WKB", false,
        Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_MultiPolygon", "_WKB", false,
        Type.GEOMETRY, Type.STRING);

    // Predicates.
    for (String fn : Arrays.asList("st_Contains", "st_Crosses", "st_Disjoint",
        "st_Equals", "st_Intersects", "st_Overlaps", "st_Touches", "st_Within")) {
      addNativeWithRelationState(db, fn, "_WKB", false, Type.BOOLEAN,
          Type.GEOMETRY, Type.GEOMETRY);
    }
    addNative(db, "st_EnvIntersects", "_WKB", false, Type.BOOLEAN,
        Type.GEOMETRY, Type.GEOMETRY);

    // Transformations.
    addNative(db, "st_Envelope", "_WKB", false, Type.GEOMETRY, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_AsText", "_WKB", false, Type.STRING, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_GeomFromText", "_WKB", false,
        Type.GEOMETRY, Type.STRING);
    addNativeWithGeomState(db, "st_GeomFromText", "_WKB", false,
        Type.GEOMETRY, Type.STRING, Type.INT);

    // Binning.
    addNative(db, "st_Bin", "Geom_WKB", false, Type.BIGINT, Type.BIGINT,
        Type.GEOMETRY);
    addNative(db, "st_Bin", "Geom_WKB", false, Type.BIGINT, Type.DOUBLE,
        Type.GEOMETRY);
    addNative(db, "st_Bin", "Wkt", false, Type.BIGINT, Type.BIGINT, Type.STRING);
    addNative(db, "st_Bin", "Wkt", false, Type.BIGINT, Type.DOUBLE, Type.STRING);
    addNative(db, "st_Binenvelope", "BinId_WKB", false, Type.GEOMETRY,
        Type.BIGINT, Type.BIGINT);
    addNative(db, "st_Binenvelope", "BinId_WKB", false, Type.GEOMETRY,
        Type.DOUBLE, Type.BIGINT);
    addNative(db, "st_Binenvelope", "Geom_WKB", false, Type.GEOMETRY,
        Type.BIGINT, Type.GEOMETRY);
    addNative(db, "st_Binenvelope", "Geom_WKB", false, Type.GEOMETRY,
        Type.DOUBLE, Type.GEOMETRY);
    addNative(db, "st_Binenvelope", "Wkt_WKB", false, Type.GEOMETRY,
        Type.BIGINT, Type.STRING);
    addNative(db, "st_Binenvelope", "Wkt_WKB", false, Type.GEOMETRY,
        Type.DOUBLE, Type.STRING);

    // Geometry property functions.
    addNativeWithGeomState(db, "st_Area", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_Length", "_WKB", false, Type.DOUBLE, Type.GEOMETRY);
    addNativeWithRelationState(db, "st_Distance", "_WKB", false, Type.DOUBLE,
        Type.GEOMETRY, Type.GEOMETRY);
    addNative(db, "st_Dimension", "_WKB", false, Type.INT, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_NumPoints", "_WKB", false, Type.INT, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_NumGeometries", "_WKB", false,
        Type.INT, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_NumInteriorRing", "_WKB", false,
        Type.INT, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_IsEmpty", "_WKB", false,
        Type.BOOLEAN, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_IsSimple", "_WKB", false,
        Type.BOOLEAN, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_IsClosed", "_WKB", false,
        Type.BOOLEAN, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_IsRing", "_WKB", false,
        Type.BOOLEAN, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_Centroid", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_StartPoint", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_EndPoint", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_PointN", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY, Type.INT);
    addNativeWithGeomState(db, "st_ExteriorRing", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY);
    addNativeWithGeomState(db, "st_InteriorRingN", "_WKB", false,
        Type.GEOMETRY, Type.GEOMETRY, Type.INT);
  }
}
