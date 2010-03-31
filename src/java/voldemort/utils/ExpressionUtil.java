/*
 * $
 * $
 *
 * Copyright (C) 1999-2009 Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package voldemort.utils;

import com.google.common.collect.MapMaker;
import org.apache.log4j.Logger;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for evaluating el expressions against serialized cache keys
 */
public class ExpressionUtil {

    private static final Logger logger = Logger.getLogger(ExpressionUtil.class);

    public static Map<String, Serializable> cache = new MapMaker().softKeys().softValues().makeMap();

    /**
     * Evaluates the supplied expression against the supply string form of a cache key and returns whether or not
     * the expression evaluates to true.
     *
     * @param elExpression the expression to apply.
     * @param serializedKey the key to test.
     * @return whether or not the supplied expression evaluates to true.
     */
    public static boolean evaluatesToTrue(String elExpression, String serializedKey) {
        try {
            Serializable compiled = getCompiledExpression(elExpression);

            Map vars = new HashMap();
            vars.put("key", serializedKey);

            // Now we execute it.
            return (Boolean) MVEL.executeExpression(compiled, vars);
        }
        catch (Exception e) {
            logger.error("Error encountered when evaluating el expression: " +  e.getMessage());
        }

        return false;
    }

    private static Serializable getCompiledExpression(String elExpression) {
        Serializable s = cache.get(elExpression);

        if (s == null) {
            s = MVEL.compileExpression(elExpression);
            cache.put(elExpression, s);
        }

        return s;
    }
}
