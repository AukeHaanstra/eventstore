package nl.pancompany.eventstore;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.CONSTRUCTOR})
public @interface StateCreator {
    String type() default "";
}
