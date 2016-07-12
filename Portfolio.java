package com.bfm.app.ignite.meetup;

public class Position {


    private final Long id;

    private final Long portfolioid;

    private final String cusip;

    private double value;


    public Position(Long id, Long portfolioid, String cusip, double value) {
        this.id = id;
        this.portfolioid = portfolioid;
        this.cusip = cusip;
        this.value = value;
    }

    public String getCusip() {
        return cusip;
    }
    public double getValue() {
        return value;
    }



    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Position [cusip=" + cusip + ", quantity=" + value + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cusip == null) ? 0 : cusip.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((portfolioid == null) ? 0 : portfolioid.hashCode());
        long temp;
        temp = Double.doubleToLongBits(value);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Position other = (Position) obj;
        if (cusip == null) {
            if (other.cusip != null)
                return false;
        } else if (!cusip.equals(other.cusip))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (portfolioid == null) {
            if (other.portfolioid != null)
                return false;
        } else if (!portfolioid.equals(other.portfolioid))
            return false;
        if (Double.doubleToLongBits(value) != Double.doubleToLongBits(other.value))
            return false;
        return true;
    }


}
