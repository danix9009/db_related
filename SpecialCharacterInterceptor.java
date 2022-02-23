package com.franky.db.common.mybatis.interceptor;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tk.mybatis.mapper.entity.Example;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.*;

/**
 * @description: 自定义拦截器方法，处理模糊查询中包含特殊字符（_、%、\）
 * @author: hd
 * @date: 2022/2
 */
@Intercepts(@Signature(type = StatementHandler.class, method = "prepare", args = {Connection.class, Integer.class}))
public class SpecialCharacterInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpecialCharacterInterceptor.class);

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        StatementHandler statementHandler = (StatementHandler) invocation.getTarget();
        BoundSql boundSql = statementHandler.getBoundSql();
        Object parameterObject = boundSql.getParameterObject();
        String sql = boundSql.getSql();
        // 处理特殊字符
        modifyLikeSql(sql, parameterObject, boundSql);
        return invocation.proceed();
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof StatementHandler && checkIfNeeded((StatementHandler) target)) {
            return Plugin.wrap(target, this);
        } else {
            return target;
        }
    }

    /**
     * 判断sql是否需要处理
     *
     * @param sql
     * @return
     */
    private boolean checkIfNeeded(String sql) {
        if (!sql.toLowerCase().contains(" like ") || !sql.toLowerCase().contains("?")) {
            return false;
        }
        return true;
    }

    private boolean checkIfNeeded(StatementHandler statementHandler) {
        BoundSql boundSql = statementHandler.getBoundSql();
        String sql = boundSql.getSql();
        return checkIfNeeded(sql);
    }

    public void modifyLikeSql(String sql, Object parameterObject, BoundSql boundSql) {
        LOGGER.debug("sql needed to modify : {}", sql);
        Map<String, ParameterMapping> keyNames = getKeyNames(sql, boundSql);
        if (parameterObject instanceof HashMap) {
            mapParamHandler(boundSql, (HashMap<String, Object>) parameterObject, keyNames);
        } else {
            commonParameterHandler(boundSql, parameterObject, keyNames.keySet());
        }
    }

    /**
     * 获取关键字的个数（去重）
     *
     * @param sql
     * @param boundSql
     * @return
     */
    private static Map<String, ParameterMapping> getKeyNames(String sql, BoundSql boundSql) {
        String[] strList = sql.split("\\?");
        Map<String, ParameterMapping> keyNames = new HashMap<>();
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        for (int i = 0; i < strList.length; i++) {
            if (strList[i].toLowerCase().contains(" like ")) {
                ParameterMapping parameterMapping = parameterMappings.get(i);
                String keyName = parameterMapping.getProperty();
                keyNames.put(keyName, parameterMapping);
            }
        }
        return keyNames;
    }

    /**
     * 是否基本数据类型或者基本数据类型的包装类
     */
    private static boolean isPrimitiveOrPrimitiveWrapper(Class<?> parameterObjectClass) {
        return parameterObjectClass.isPrimitive() || (parameterObjectClass.isAssignableFrom(Byte.class)
                || parameterObjectClass.isAssignableFrom(Short.class)
                || parameterObjectClass.isAssignableFrom(Integer.class)
                || parameterObjectClass.isAssignableFrom(Long.class)
                || parameterObjectClass.isAssignableFrom(Double.class)
                || parameterObjectClass.isAssignableFrom(Float.class)
                || parameterObjectClass.isAssignableFrom(Character.class)
                || parameterObjectClass.isAssignableFrom(Boolean.class));
    }

    /**
     * 处理实体类场景
     */
    private void commonParameterHandler(BoundSql boundSql, Object parameterObject, Set<String> keyNames) {
        Class<?> parameterObjectClass = parameterObject.getClass();
        for (String keyName : keyNames) {
            if (!isPrimitiveOrPrimitiveWrapper(parameterObjectClass)) {
                if (keyName.contains("__frch_criterion")) {
                    //process param value assembled by tk.mapper
                    Object value;
                    if (keyName.contains(".value")) {
                        keyName = keyName.replace(".value", "");
                        Example.Criterion criterion = (Example.Criterion) boundSql.getAdditionalParameter(keyName);
                        value = criterion.getValue();
                        if (!(value instanceof String)) {
                            continue;
                        }
                        String s = processPrefixOrSuffixExistString((String) value);
                        try {
                            Field field = criterion.getClass().getDeclaredField("value");
                            field.setAccessible(true);
                            field.set(criterion, s);
                        } catch (NoSuchFieldException | IllegalAccessException e) {
                            LOGGER.warn("字段替换值错误", e);
                        }
                    } else {
                        value = boundSql.getAdditionalParameter(keyName);
                        if (!(value instanceof String)) {
                            continue;
                        }
                        String s = processPrefixOrSuffixExistString((String) value);
                        boundSql.setAdditionalParameter(keyName, s);
                    }
                } else {
                    try {
                        Field field = parameterObjectClass.getDeclaredField(keyName);
                        field.setAccessible(true);
                        String propertyValue = String.valueOf(field.get(parameterObject));
                        field.set(parameterObject, escapeChar(propertyValue));
                    } catch (NoSuchFieldException | IllegalAccessException e) {
                        LOGGER.warn("字段替换值错误", e);
                    }
                }
            }
        }
    }

    /**
     * 处理自动生成的参数值
     *
     * @param value
     */
    private String processPrefixOrSuffixExistString(String value) {
        boolean prefixExist = false;
        boolean suffixExist = false;
        String s = value;
        if (s.length() < 1) {
            return s;
        }
        if (s.charAt(0) == '%') {
            prefixExist = true;
            s = s.substring(1);
        }
        if (s.charAt(s.length() - 1) == '%') {
            suffixExist = true;
            s = s.substring(0, s.length() - 1);
        }
        if (!s.contains("_") && !s.contains("\\") && !s.contains("%")) {
            //return the original string
            return value;
        }
        String newStr = escapeChar(s);
        if (prefixExist) {
            newStr = "%" + newStr;
        }
        if (suffixExist) {
            newStr = newStr + "%";
        }
        return newStr;
    }

    private void mapParamHandler(BoundSql boundSql, Map<String, Object> parameterObject, Map<String, ParameterMapping> keyNames) {
        for (Map.Entry<String, ParameterMapping> entry : keyNames.entrySet()) {
            String keyName = entry.getKey();
            ParameterMapping parameterMapping = entry.getValue();

            Class<?> javaType = parameterMapping.getJavaType();
            if (javaType == Object.class) {
                //处理@Param参数里的属性
                if (skipPageQueryAvoidTwice(parameterObject)) {
                    continue;
                }
                String[] split = keyName.split("\\.");
                int length = split.length;
                if (length == 0) {
                    continue;
                }
                Object r = parameterObject.get(split[0]);
                String finalKey = split[length - 1];
                Deque<Field> finalFields = new LinkedList<>();
                try {
                    for (int i = 1; i < length; i++) {
                        Field field = r.getClass().getDeclaredField(split[i]);
                        finalFields.push(field);
                        field.setAccessible(true);
                        r = field.get(r);
                        if (null == r) {
                            finalFields.clear();
                            break;
                        }
                    }
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    LOGGER.warn("取字段错误", e);
                    continue;
                }

                if (CollectionUtils.isEmpty(finalFields)) {
                    continue;
                }
                if (r instanceof String && (r.toString().contains("_") || r.toString().contains("\\") || r.toString()
                        .contains("%"))) {
                    try {
                        String s = escapeChar((String) r);
                        if (finalFields.size() == 1) {
                            Object origin = parameterObject.get(split[0]);
                            Field f = origin.getClass().getDeclaredField(finalKey);
                            f.setAccessible(true);
                            f.set(origin, s);
                        } else {
                            finalFields.pop();
                            finalFields.pop().set(finalKey, s);
                        }
                    } catch (IllegalAccessException | NoSuchFieldException e) {
                        LOGGER.warn("设置字段错误", e);
                    }
                }
            } else {
                Object value = boundSql.getAdditionalParameter(keyName);
                //process <bind> type parameter
                if (value instanceof String) {
                    String s = processPrefixOrSuffixExistString((String) value);
                    boundSql.setAdditionalParameter(keyName, s);
                    continue;
                }
                if (skipPageQueryAvoidTwice(parameterObject)) {
                    continue;
                }
                Object a = parameterObject.get(keyName);
                if (a instanceof String && (a.toString().contains("_") || a.toString().contains("\\") || a.toString()
                        .contains("%"))) {
                    parameterObject.put(keyName, escapeChar(a.toString()));
                }
            }
        }
    }

    //skip page query because the param value has been modified when count
    private boolean skipPageQueryAvoidTwice(Map<String, Object> parameterObject) {
        if (parameterObject.containsKey("First_PageHelper") || parameterObject.containsKey("Second_PageHelper")) {
            return true;
        }
        return false;
    }

    //特殊字符转义
    private static String escapeChar(String before) {
        if (StringUtils.isNotBlank(before)) {
            before = before.replace("\\", "\\\\");
            before = before.replace("_", "\\_");
            before = before.replace("%", "\\%");
        }
        return before;
    }
}

