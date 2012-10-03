from sqlalchemy import select, func
from sqlalchemy.sql import and_, text, column, table

from geoalchemy.base import SpatialComparator, PersistentSpatialElement, \
    WKBSpatialElement, WKTSpatialElement
from geoalchemy.dialect import SpatialDialect 
from geoalchemy.functions import functions, BaseFunction
from geoalchemy.mysql import mysql_functions
from geoalchemy.geometry import GeometryExtensionColumn


class GeoDBComparator(SpatialComparator):
    """Comparator class used for Spatialite
    """
    def __getattr__(self, name):
        try:
            return SpatialComparator.__getattr__(self, name)
        except AttributeError:
            return getattr(geodb_functions, name)(self)


class GeoDBPersistentSpatialElement(PersistentSpatialElement):
    """Represents a Geometry value as loaded from the database."""
    
    def __init__(self, desc):
        self.desc = desc
        
    def __getattr__(self, name):
        try:
            return PersistentSpatialElement.__getattr__(self, name)
        except AttributeError:
            return getattr(geodb_functions, name)(self)


class geodb_functions(mysql_functions):
    """Functions only supported by GeoDB.
    """
    
    class svg(BaseFunction):
        """AsSVG(g)"""
        pass
    
    class fgf(BaseFunction):
        """AsFGF(g)"""
        pass

    @staticmethod
    def _within_distance(compiler, geom1, geom2, distance, *args):
        if isinstance(geom1, GeometryExtensionColumn) and \
           geom1.type.spatial_index and \
           GeoDBSpatialDialect.supports_rtree(compiler.dialect):
            """If querying on a geometry column that also has a spatial index,
            then make use of this index.
            """
            return and_(
                func.Distance(geom1, geom2) <= distance,
                table(geom1.table.fullname, column("rowid")).c.rowid.in_(
                    select([table("idx_%s_%s" % (geom1.table.fullname, geom1.key), column("pkid")).c.pkid]).where(
                        and_(text('xmin') >= func.MbrMinX(geom2) - distance,
                        and_(text('xmax') <= func.MbrMaxX(geom2) + distance,
                        and_(text('ymin') >= func.MbrMinY(geom2) - distance,
                             text('ymax') <= func.MbrMaxY(geom2) + distance))))))
        else:
            return func.Distance(geom1, geom2) <= distance


class GeoDBSpatialDialect(SpatialDialect):
    """Implementation of SpatialDialect for GeoDB."""
    
    __functions = { 
        WKTSpatialElement: 'ST_GeomFromText',
        WKBSpatialElement: 'ST_GeomFromWKB',
        functions.wkt: 'ST_AsText',
        functions.wkb: '',
        functions.dimension : 'ST_Dimension',
        functions.srid : 'ST_SRID',
        functions.geometry_type : 'ST_GeometryType',
        functions.is_valid : 'ST_IsValid',
        functions.is_empty : 'ST_IsEmpty',
        functions.is_simple : 'ST_IsSimple',
        functions.is_closed : 'ST_IsClosed',
        functions.is_ring : 'ST_IsRing',
        functions.num_points : 'ST_NumPoints',
        functions.point_n : 'ST_PointN',
        functions.length : 'ST_Length',
        functions.area : 'ST_Area',
        functions.x : 'ST_X',
        functions.y : 'ST_Y',
        functions.centroid : 'ST_Centroid',
        functions.boundary : 'ST_Boundary',
        functions.buffer : 'ST_Buffer',
        functions.convex_hull : 'ST_ConvexHull',
        functions.envelope : 'ST_Envelope',
        functions.start_point : 'ST_StartPoint',
        functions.end_point : 'ST_EndPoint',
        functions.transform : 'ST_Transform',
        functions.equals : 'ST_Equals',
        functions.distance : 'ST_Distance',
        functions.within_distance : 'ST_DWithin',
        functions.disjoint : 'ST_Disjoint',
        functions.intersects : 'ST_Intersects',
        functions.touches : 'ST_Touches',
        functions.crosses : 'ST_Crosses',
        functions.within : 'ST_Within',
        functions.overlaps : 'ST_Overlaps',
        functions.gcontains : 'ST_Contains',
        functions.covers : 'ST_Covers',
        functions.covered_by : 'ST_CoveredBy',
        functions.intersection : 'ST_Intersection',
        functions.union : 'ST_Union',
        functions.collect : 'ST_Collect',
        functions.extent : 'ST_Extent',
        # not tested
        #functions.aggregate_union : 'GUnion',
        geodb_functions.svg : 'AsSVG',
        geodb_functions.fgf : 'AsFGF',
        mysql_functions.mbr_equal : 'MBREqual',
        mysql_functions.mbr_disjoint : 'MBRDisjoint',
        mysql_functions.mbr_intersects : 'MBRIntersects',
        mysql_functions.mbr_touches : 'MBRTouches',
        mysql_functions.mbr_within : 'MBRWithin',
        mysql_functions.mbr_overlaps : 'MBROverlaps',
        mysql_functions.mbr_contains : 'MBRContains',
        functions._within_distance : geodb_functions._within_distance
    }

    def _get_function_mapping(self):
        return GeoDBSpatialDialect.__functions
    
    def process_result(self, value, type):
        return GeoDBPersistentSpatialElement(WKBSpatialElement(value, type.srid))
    
    def handle_ddl_before_drop(self, bind, table, column):
        if column.type.spatial_index and GeoDBSpatialDialect.supports_rtree(bind.dialect):
            bind.execute(select([func.DisableSpatialIndex(table.name, column.name)]).execution_options(autocommit=True))
            bind.execute("""
                         DROP TABLE idx_%s_%s
                         """ % (table.name, column.name))
    
    def handle_ddl_after_create(self, bind, table, column):
        bind.execute("""
                     ALTER TABLE %s ADD %s BLOB
                     """ % (table.name, column.name,) )
        if column.type.spatial_index and GeoDBSpatialDialect.supports_rtree(bind.dialect):
            bind.execute("""
                         SELECT CreateSpatialIndex(null, '"%s"', '"%s"', '"%s"')"
                         """ % (table.name, column.name, column.type.srid))
    
    @staticmethod  
    def supports_rtree(dialect):
        return False  
