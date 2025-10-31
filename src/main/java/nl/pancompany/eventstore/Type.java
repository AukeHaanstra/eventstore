package nl.pancompany.eventstore;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

public record Type(String type, Class<?> clazz) {

    public Type(String type) {
        this(type, Object.class);
    }

    public Type(Class<?> clazz) {
        this(getName(clazz), clazz);
    }

    public static Type of(String type) {
        return new Type(type);
    }

    public static Type of(Class<?> clazz) {
        return new Type(clazz);
    }

    private static String getName(Class<?> clazz) {
        if (clazz.getCanonicalName() == null) {
            throw new IllegalArgumentException("Only classes with canonical names allowed for auto event-typing.");
        }
        return clazz.getCanonicalName();
    }

    public Types orType(String type) {
        return new Types(Set.of(this, new Type(type)));
    }

    public Types orType(Class<?> clazz) {
        return new Types(Set.of(this, Type.of(clazz)));
    }

    public static Type getTypeForAnnotatedParameter(Annotation annotation, Class<?> declaredParemeterType) {
        String parameterName = annotation.getClass().getSimpleName();
        String type = getAnnotationTypeElementValue(annotation);
        if (declaredParemeterType == Object.class && type.isBlank()) {
            throw new IllegalArgumentException("%s annotation must have a type defined when the first parameter is Object."
                    .formatted(parameterName));
        } else if (declaredParemeterType != Object.class && !type.isBlank()) {
            throw new IllegalArgumentException(String.format("Either declare an @%s(type = ..) with an Object " +
                    "parameter, or declare @%s with a typed parameter.", parameterName, parameterName));
        } else if (declaredParemeterType == Object.class) {
            return Type.of(type);
        }
        return Type.of(declaredParemeterType);
    }

    private static String getAnnotationTypeElementValue(Annotation annotation) {
        String type;
        try {
            Method getType = annotation.getClass().getMethod("type");
            getType.setAccessible(true);
            type = (String) getType.invoke(annotation);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
        return type;
    }

}
