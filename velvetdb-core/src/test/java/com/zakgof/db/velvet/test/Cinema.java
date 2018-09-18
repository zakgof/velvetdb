package com.zakgof.db.velvet.test;

import java.time.LocalDate;
import java.util.Comparator;

import com.zakgof.db.velvet.entity.Entities;
import com.zakgof.db.velvet.entity.IEntityDef;
import com.zakgof.db.velvet.entity.IKeylessEntityDef;
import com.zakgof.db.velvet.join.JoinDef;
import com.zakgof.db.velvet.link.IBiManyToManyLinkDef;
import com.zakgof.db.velvet.link.IBiMultiLinkDef;
import com.zakgof.db.velvet.link.IBiSecMultiLinkDef;
import com.zakgof.db.velvet.link.ISingleLinkDef;
import com.zakgof.db.velvet.link.Links;

class Movie {
    @Override
    public String toString() {
        return "Movie [title=" + title + ", rating=" + rating + ", releaseDate=" + releaseDate + "]";
    }
    public Movie(String title, String rating, LocalDate releaseDate) {
        this.title = title;
        this.rating = rating;
        this.releaseDate = releaseDate;
    }

    public String getTitle() {
        return title;
    }
    public String getRating() {
        return rating;
    }
    public LocalDate getReleaseDate() {
        return releaseDate;
    }

    private final String title;
    private final String rating;
    private final LocalDate releaseDate;
}

class Person {
    @Override
    public String toString() {
        return "Person [passportNo=" + passportNo + ", name=" + name + ", birthDate=" + birthDate + "]";
    }

    public Person(String passportNo, String name, LocalDate birthDate) {
        this.passportNo = passportNo;
        this.name = name;
        this.birthDate = birthDate;
    }

    public String getPassportNo() {
        return passportNo;
    }
    public String getName() {
        return name;
    }
    public LocalDate getBirthDate() {
        return birthDate;
    }

    private final String passportNo;
    private final String name;
    private final LocalDate birthDate;
}

class Company {
    @Override
    public String toString() {
        return "Company [name=" + name + "]";
    }

    public String getName() {
        return name;
    }

    public Company(String name) {
        this.name = name;
    }

    private final String name;
}

class Feedback {
    public double getAvgScore() {
        return avgScore;
    }
    public long getVotes() {
        return votes;
    }
    @Override
    public String toString() {
        return "Feedback [avgScore=" + avgScore + ", votes=" + votes + "]";
    }
    public Feedback(double avgScore, long votes) {
        this.avgScore = avgScore;
        this.votes = votes;
    }
    private final double avgScore;
    private final long votes;
}

class CinemaDefs {
    static IKeylessEntityDef<Movie> MOVIE = Entities.keyless(Movie.class);
    static IEntityDef<String, Person> PERSON = Entities.from(Person.class).make(String.class, Person::getPassportNo);
    static IEntityDef<String, Company> COMPANY = Entities.from(Company.class).make(String.class, Company::getName);
    static IKeylessEntityDef<Feedback> FEEDBACK = Entities.keyless(Feedback.class);

    static IBiSecMultiLinkDef<String, Company, Long, Movie, LocalDate> COMPANY_MOVIES = Links.biSec(COMPANY, MOVIE, LocalDate.class, Movie::getReleaseDate);
    static ISingleLinkDef<Long, Movie, Long, Feedback> MOVIE_FEEDBACK = Links.single(MOVIE, FEEDBACK);
    static IBiMultiLinkDef<String, Person, Long, Movie> DIRECTOR_MOVIE = Links.biMulti(PERSON, MOVIE, "director-movie", "movie-director");
    static IBiMultiLinkDef<String, Person, Long, Movie> PRODUCER_MOVIE = Links.biMulti(PERSON, MOVIE, "producer-movie", "movie-producer");
    static IBiManyToManyLinkDef<Long, Movie, String, Person> MOVIE_CAST = Links.biManyToMany(MOVIE, PERSON, "movie-actor", "actor-movie");

    static JoinDef<String, Company> COMPANY_REVIEW = JoinDef.builderFor(COMPANY).include(COMPANY_MOVIES).done()
        .entity(MOVIE).include(MOVIE_FEEDBACK).done()
        .entity(FEEDBACK).done()
        .build();

    static JoinDef<Long, Movie> MOVIE_DETAILS = JoinDef.builderFor(MOVIE)
        .include(MOVIE_CAST)
        .include(MOVIE_FEEDBACK)
        .include(DIRECTOR_MOVIE.back())
        .include(PRODUCER_MOVIE.back())
        .include(COMPANY_MOVIES.back())
        .sort(Comparator.comparing(Movie::getReleaseDate))
        .done().build();

    static JoinDef<String, Company> DELETE_COMPANY = JoinDef.builderFor(COMPANY)
        .include(COMPANY_MOVIES)
        .done()
            .entity(MOVIE)
                 .include(MOVIE_FEEDBACK)
                 .detach(DIRECTOR_MOVIE.back())
                 .detach(PRODUCER_MOVIE.back())
                 .detach(MOVIE_CAST)
            .done()
        .build();

}
