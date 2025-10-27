package nl.pancompany.eventstore;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR})
public @interface StateConstructor {
    String type() default "";
}
