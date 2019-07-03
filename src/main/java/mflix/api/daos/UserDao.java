package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static com.mongodb.client.model.Filters.eq;

@Configuration
public class UserDao extends AbstractMFlixDao {

  private final MongoCollection<User> usersCollection;
  private final MongoCollection<Session> sessionsCollection;

  //TODO> Ticket: User Management - do the necessary changes so that the sessions collection
  //returns a Session object

  private final Logger log;

  @Autowired
  public UserDao(
      MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
    super(mongoClient, databaseName);
    CodecRegistry pojoCodecRegistry =
        fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

    log = LoggerFactory.getLogger(this.getClass());

    usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);

    //TODO> Ticket: User Management - implement the necessary changes so that the sessions
    // collection returns a Session objects instead of Document objects.
    sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
  }

  /**
   * Inserts the `user` object in the `users` collection.
   *
   * @param user - User object to be added
   * @return True if successful, throw IncorrectDaoOperation otherwise
   */
  public boolean addUser(User user) {

    if (getUser(user.getEmail()) != null) {
      throw new IncorrectDaoOperation("User already exist");
    }

    usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);
    return getUser(user.getEmail()) != null;

    //TODO > Ticket: Durable Writes -  you might want to use a more durable write concern here!
    //TODO > Ticket: Handling Errors - make sure to only add new users
    // and not users that already exist.
  }

  /**
   * Creates session using userId and jwt token.
   *
   * @param userId - user string identifier
   * @param jwt - jwt string token
   * @return true if successful
   */
  public boolean createUserSession(String userId, String jwt) {

    Session session = new Session();
    session.setUserId(userId);
    session.setJwt(jwt);

    if (getUserSession(userId) != null) {
      throw new IncorrectDaoOperation("Session with given jwt already exist");
    }

    sessionsCollection.insertOne(session);
    return true;

    //TODO> Ticket: User Management - implement the method that allows session information to be
    // stored in it's designated collection.
    //TODO > Ticket: Handling Errors - implement a safeguard against
    // creating a session with the same jwt token.
  }

  /**
   * Returns the User object matching the an email string value.
   *
   * @param email - email string to be matched.
   * @return User object or null.
   */
  public User getUser(String email) {
    User user = null;

    MongoCursor<User> it = usersCollection.find(eq("email", email)).iterator();
    if (it.hasNext()) {
      user = it.next();
    }
    return user;

    //TODO> Ticket: User Management - implement the query that returns the first User object.
  }

  /**
   * Given the userId, returns a Session object.
   *
   * @param userId - user string identifier.
   * @return Session object or null.
   */
  public Session getUserSession(String userId) {
    Session session = null;

    MongoCursor<Session> it = sessionsCollection.find(eq("user_id", userId)).iterator();
    if (it.hasNext()) {
      session = it.next();
    }
    return session;

    //TODO> Ticket: User Management - implement the method that returns Sessions for a given
    // userId
  }

  public boolean deleteUserSessions(String userId) {
    DeleteResult result = sessionsCollection.deleteOne(eq("user_id", userId));
    return result.wasAcknowledged();

    //TODO> Ticket: User Management - implement the delete user sessions method
  }

  /**
   * Removes the user document that match the provided email.
   *
   * @param email - of the user to be deleted.
   * @return true if user successfully removed
   */
  public boolean deleteUser(String email) {
    DeleteResult userResult = usersCollection.deleteOne(eq("email", email));
    return userResult.wasAcknowledged() && deleteUserSessions(email);

    //TODO> Ticket: User Management - implement the delete user method and it's session
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions.
  }

  /**
   * Updates the preferences of an user identified by `email` parameter.
   *
   * @param email - user to be updated email
   * @param userPreferences - set of preferences that should be stored and replace the existing
   *     ones. Cannot be set to null value
   * @return User object that just been updated.
   */
  public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
    //TODO> Ticket: User Preferences - implement the method that allows for user preferences to
    // be updated.
    //TODO > Ticket: Handling Errors - make this method more robust by
    // handling potential exceptions when updating an entry.
    return false;
  }
}
