package com.yinrs.rank;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RankManagerFactory {
    public static <T extends RankManager> T getInstance(Class<T> clazz) {
        try {
            Method instance = clazz.getDeclaredMethod("instance");
            instance.setAccessible(true);
            return (T) instance.invoke(null);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } finally {
        }
        return null;
    }
}