/**
 * Created by vvakhlyuev on 02/07/2017.
 */

import java.util.Random;

/**
 * Generate pseudo-random floating point values, with an
 * approximately Gaussian (normal) distribution.
 * <p>
 * Many physical measurements have an approximately Gaussian
 * distribution; this provides a way of simulating such values.
 */
public final class RandomGaussian {

    private Random fRandom = new Random();
    private double mean;
    private double stddev;
    private double lowerLimit;
    private double upperLimit;
    private double precision;

    public RandomGaussian(double mean, double stddev, double lowerLimit, double upperLimit) {
        this.mean = mean;
        this.stddev = stddev;
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;
    }

    public RandomGaussian(double mean, double stddev, double lowerLimit, double upperLimit, double precision) {
        this.mean = mean;
        this.stddev = stddev;
        this.lowerLimit = lowerLimit;
        this.upperLimit = upperLimit;
        this.precision = precision;
    }

    public void setPrecision(double precision) {
        this.precision = precision;
    }

    public double getGaussianDouble() {
        double num = mean + fRandom.nextGaussian() * stddev;
        num = applyPrecision(num);

        while (num < lowerLimit || num > upperLimit) {
            num = mean + fRandom.nextGaussian() * stddev;
            num = applyPrecision(num);
        }
        return num;
    }

    public int getGaussianInt() {
        int num = (int) Math.floor(mean + fRandom.nextGaussian() * stddev);
        while (num < lowerLimit || num > upperLimit) {
            num = (int) Math.floor(mean + fRandom.nextGaussian() * stddev);
        }
        return num;
    }

    private double applyPrecision(double num) {
        if (precision > 0) {
            num = Math.floor(num * Math.pow(10, precision)) / Math.pow(10, precision);
        }
        return num;
    }

}
