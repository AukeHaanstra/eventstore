package nl.pancompany.eventstore;

import java.lang.reflect.Method;

record InvocableMethod(Object objectWithMethod, Method method) {
}
