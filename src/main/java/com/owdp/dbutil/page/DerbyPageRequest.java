package com.owdp.dbutil.page;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
/**
 * Derby数据库的分页查询实现
 */
public class DerbyPageRequest extends PageRequest {
    private static final long serialVersionUID = 2080442062144491711L;

    /**
     * 初始化分页查询
     * @param pageNumber 页码编号，从1开始
     * @param pageSize 单页数量
     */
    public DerbyPageRequest(int pageNumber, int pageSize) {
        super(pageNumber, pageSize);
    }

    private static boolean hasForUpdateClause(int forUpdateIndex) {
        return forUpdateIndex >= 0;
    }

    private static boolean hasWithClause(String normalizedSelect){
        return normalizedSelect.startsWith( "with ", normalizedSelect.length() - 7 );
    }

    private static int getWithIndex(String querySelect) {
        int i = querySelect.lastIndexOf( "with " );
        if ( i < 0 ) {
            i = querySelect.lastIndexOf( "WITH " );
        }
        return i;
    }


    @Override
    public String pageSelect(String query) {
        final StringBuilder sb = new StringBuilder(query.length() + 50);
        final String normalizedSelect = query.toLowerCase(Locale.ROOT).trim();
        final int forUpdateIndex = normalizedSelect.lastIndexOf( "for update") ;

        if ( hasForUpdateClause( forUpdateIndex ) ) {
            sb.append( query.substring( 0, forUpdateIndex-1 ) );
        }
        else if ( hasWithClause( normalizedSelect ) ) {
            sb.append( query.substring( 0, getWithIndex( query ) - 1 ) );
        }
        else {
            sb.append( query );
        }

        if ( offset() == 0 ) {
            sb.append( " fetch first " );
        }
        else {
            sb.append( " offset " ).append( offset() ).append( " rows fetch next " );
        }

        sb.append( limit() ).append( " rows only" );

        if ( hasForUpdateClause( forUpdateIndex ) ) {
            sb.append( ' ' );
            sb.append( query.substring( forUpdateIndex ) );
        }
        else if ( hasWithClause( normalizedSelect ) ) {
            sb.append( ' ' ).append( query.substring( getWithIndex( query ) ) );
        }
        return sb.toString();
    }

    @Override
    public void fillState(PreparedStatement ps, int index) throws SQLException {

    }
}
