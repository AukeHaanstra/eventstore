package nl.pancompany.eventstore.annotation;

import nl.pancompany.eventstore.query.Tag;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface EventHandler {
    String type() default "";
    String[] tags() default {};
    boolean enableReplay() default false;
}
