package com.zakgof.db.velvet.test;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zakgof.db.velvet.join.DataWrap;


public class JoinTest extends AVelvetTxnTest {

    @Before
    public void init() {

        Company paramount = new Company("Paramount");
        Company columbia = new Company("Columbia Pictures");

        Movie term1    = new Movie("Terminator", "R", LocalDate.of(1984,  9,  26));
        Movie term2    = new Movie("Terminator 2", "R", LocalDate.of(1991,  7,   3));
        Movie titanic  = new Movie("Titanic", "M", LocalDate.of(1997,  12,   19));
        Movie ides    =  new Movie("The Ides of March", "N",  LocalDate.of(2011,  1,   1));

        Person hurd     = new Person("GH34343",  "Gale Anne Hurd", LocalDate.of(1900,  1,  1));
        Person cameron  = new Person("JC123456", "James Cameron", LocalDate.of(1954,  8,  16));
        Person schwarz  = new Person("AS638743", "Arnold Schwarzenegger", LocalDate.of(1900,  1,  1));
        Person dicaprio = new Person("LD528374", "Leonardo DiCaprio", LocalDate.of(1900,  1,  1));
        Person clooney  = new Person("GC745667", "George Clooney", LocalDate.of(1900,  1,  1));
        Person hamilton = new Person("LH010101", "Linda Hamilton", LocalDate.of(1900,  1,  1));
        Person winslet  = new Person("KW745667", "Kate Winslet", LocalDate.of(1900,  1,  1));
        Person gosling  = new Person("RG592774", "Ryan Gosling", LocalDate.of(1900,  1,  1));

        Feedback fb1 = new Feedback(8.0, 100000L);
        Feedback fb2 = new Feedback(7.0, 200000L);
        Feedback fb3 = new Feedback(6.0, 300000L);
        Feedback fb4 = new Feedback(5.0, 400000L);

        CinemaDefs.COMPANY.batchPut(velvet, Arrays.asList(paramount, columbia));
        CinemaDefs.MOVIE.batchPut(velvet, Arrays.asList(ides, term1, term2, titanic));
        CinemaDefs.PERSON.batchPut(velvet, Arrays.asList(hurd, cameron, schwarz, dicaprio, clooney, hamilton, winslet, gosling));
        CinemaDefs.FEEDBACK.batchPut(velvet, Arrays.asList(fb1, fb2, fb3, fb4));

        CinemaDefs.MOVIE_FEEDBACK.connect(velvet, term1, fb1);
        CinemaDefs.MOVIE_FEEDBACK.connect(velvet, term2, fb2);
        CinemaDefs.MOVIE_FEEDBACK.connect(velvet, titanic, fb3);
        CinemaDefs.MOVIE_FEEDBACK.connect(velvet, ides, fb4);

        CinemaDefs.COMPANY_MOVIES.connect(velvet, paramount, term1);
        CinemaDefs.COMPANY_MOVIES.connect(velvet, paramount, term2);
        CinemaDefs.COMPANY_MOVIES.connect(velvet, columbia, titanic);
        CinemaDefs.COMPANY_MOVIES.connect(velvet, columbia, ides);

        CinemaDefs.DIRECTOR_MOVIE.connect(velvet, cameron, term1);
        CinemaDefs.DIRECTOR_MOVIE.connect(velvet, cameron, term2);
        CinemaDefs.DIRECTOR_MOVIE.connect(velvet, cameron, titanic);
        CinemaDefs.DIRECTOR_MOVIE.connect(velvet, clooney, ides);

        CinemaDefs.PRODUCER_MOVIE.connect(velvet, hurd, term1);
        CinemaDefs.PRODUCER_MOVIE.connect(velvet, cameron, term2);
        CinemaDefs.PRODUCER_MOVIE.connect(velvet, cameron, titanic);
        CinemaDefs.PRODUCER_MOVIE.connect(velvet, dicaprio, ides);

        CinemaDefs.MOVIE_CAST.connect(velvet, term1, schwarz);
        CinemaDefs.MOVIE_CAST.connect(velvet, term2, schwarz);
        CinemaDefs.MOVIE_CAST.connect(velvet, ides, clooney);
        CinemaDefs.MOVIE_CAST.connect(velvet, ides, gosling);
        CinemaDefs.MOVIE_CAST.connect(velvet, titanic, dicaprio);
        CinemaDefs.MOVIE_CAST.connect(velvet, titanic, winslet);
        CinemaDefs.MOVIE_CAST.connect(velvet, term1, hamilton);
        CinemaDefs.MOVIE_CAST.connect(velvet, term2, hamilton);
    }

    @Test
    public void testJoinGet() {
       DataWrap<String, Company> companyReview = CinemaDefs.COMPANY_REVIEW.get(velvet, "Columbia Pictures");
       Assert.assertNotNull(companyReview);

       Assert.assertEquals("Columbia Pictures", companyReview.getKey());
       Assert.assertEquals("Columbia Pictures", companyReview.getNode().getName());

       List<DataWrap<Long, Movie>> movies = companyReview.multiLink(CinemaDefs.COMPANY_MOVIES);
       Assert.assertEquals("Titanic", movies.get(0).getNode().getTitle());
       Assert.assertEquals("The Ides of March", movies.get(1).getNode().getTitle());

       Assert.assertEquals(6.0,  movies.get(0).singleLink(CinemaDefs.MOVIE_FEEDBACK).getNode().getAvgScore(), 0.0001);
       Assert.assertEquals(5.0,  movies.get(1).singleLink(CinemaDefs.MOVIE_FEEDBACK).getNode().getAvgScore(), 0.0001);
    }

    @Test
    public void testJoinGetAll() {
        List<DataWrap<Long, Movie>> movieWraps = CinemaDefs.MOVIE_DETAILS.batchGetAll(velvet);
        Assert.assertEquals("Terminator", movieWraps.get(0).getNode().getTitle());
        Assert.assertEquals("Terminator 2", movieWraps.get(1).getNode().getTitle());
        Assert.assertEquals("Titanic", movieWraps.get(2).getNode().getTitle());
        Assert.assertEquals("The Ides of March", movieWraps.get(3).getNode().getTitle());

        List<DataWrap<String, Person>> t2cast = movieWraps.get(1).multiLink(CinemaDefs.MOVIE_CAST);
        Set<String> t2castnames = t2cast.stream().map(DataWrap::getNode).map(Person::getName).collect(Collectors.toSet());
        Assert.assertEquals(new HashSet<>(Arrays.asList("Linda Hamilton", "Arnold Schwarzenegger")), t2castnames);

        Assert.assertEquals("James Cameron", movieWraps.get(1).singleLink(CinemaDefs.PRODUCER_MOVIE.back()).getNode().getName());
        Assert.assertEquals("James Cameron", movieWraps.get(1).singleLink(CinemaDefs.DIRECTOR_MOVIE.back()).getNode().getName());
        Assert.assertEquals("Paramount", movieWraps.get(1).singleLink(CinemaDefs.COMPANY_MOVIES.back()).getNode().getName());

        Assert.assertEquals("James Cameron", movieWraps.get(2).singleLink(CinemaDefs.PRODUCER_MOVIE.back()).getNode().getName());
        Assert.assertEquals("James Cameron", movieWraps.get(2).singleLink(CinemaDefs.DIRECTOR_MOVIE.back()).getNode().getName());
        Assert.assertEquals("Columbia Pictures", movieWraps.get(2).singleLink(CinemaDefs.COMPANY_MOVIES.back()).getNode().getName());

        Assert.assertEquals("Leonardo DiCaprio", movieWraps.get(3).singleLink(CinemaDefs.PRODUCER_MOVIE.back()).getNode().getName());
        Assert.assertEquals("George Clooney", movieWraps.get(3).singleLink(CinemaDefs.DIRECTOR_MOVIE.back()).getNode().getName());
        Assert.assertEquals("Columbia Pictures", movieWraps.get(3).singleLink(CinemaDefs.COMPANY_MOVIES.back()).getNode().getName());
    }

    @Test
    public void testDelete() {
        CinemaDefs.DELETE_COMPANY.deleteKey(velvet, "Paramount");
        Assert.assertEquals(set("Columbia Pictures"), new HashSet<>(CinemaDefs.COMPANY.batchGetAllKeys(velvet)));
        Assert.assertEquals(2, CinemaDefs.MOVIE.size(velvet));
        Assert.assertEquals(8, CinemaDefs.PERSON.size(velvet));
        Assert.assertEquals(2, CinemaDefs.FEEDBACK.size(velvet));
        Assert.assertEquals(1, CinemaDefs.DIRECTOR_MOVIE.keys(velvet, "JC123456").size());
        Assert.assertEquals(0, CinemaDefs.MOVIE_CAST.back().keys(velvet, "AS638743").size());
    }

    private Set<String> set(String... strs) {
        return new HashSet<>(Arrays.asList(strs));
    }

}
