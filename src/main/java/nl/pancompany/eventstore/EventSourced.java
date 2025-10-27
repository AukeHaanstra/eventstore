package nl.pancompany.eventstore;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface EventSourced {
    String type() default "";
}
