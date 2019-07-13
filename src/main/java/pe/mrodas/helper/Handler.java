package pe.mrodas.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Handler {

    @FunctionalInterface
    public interface ThrowingFunction<T, R> {
        R apply(T arg) throws Exception;
    }

    @FunctionalInterface
    public interface ThrowingBiFunction<S, T, R> {
        R apply(S arg1, T arg2) throws Exception;
    }

    public static <T, R> List<R> handle(ThrowingFunction<T[], List<R>> function, T[] input, Consumer<Exception> onError) {
        try {
            return function.apply(input);
        } catch (Exception e) {
            onError.accept(e);
        }
        return new ArrayList<>();
    }

    public static <T, R> List<R> handle(ThrowingFunction<T, List<R>> function, T input, Consumer<Exception> onError) {
        try {
            return function.apply(input);
        } catch (Exception e) {
            onError.accept(e);
        }
        return new ArrayList<>();
    }

    public static <T, R> R handle(ThrowingFunction<T, R> function, T input, Supplier<R> resultOnError, Consumer<Exception> onError) {
        try {
            return function.apply(input);
        } catch (Exception e) {
            onError.accept(e);
        }
        return resultOnError.get();
    }

    public static <S, T, R> R handle(ThrowingBiFunction<S, T, R> function, S input1, T input2, Supplier<R> resultOnError, Consumer<Exception> onError) {
        try {
            return function.apply(input1, input2);
        } catch (Exception e) {
            onError.accept(e);
        }
        return resultOnError.get();
    }

    public static <S, T, R> List<R> handle(ThrowingBiFunction<S, T, List<R>> function, S input1, T input2, Consumer<Exception> onError) {
        try {
            return function.apply(input1, input2);
        } catch (Exception e) {
            onError.accept(e);
        }
        return new ArrayList<>();
    }
}
