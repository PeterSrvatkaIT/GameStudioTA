package sk.tuke.gamestudio.entity;

import java.io.Serializable;
import java.util.Date;

public class Comment implements Serializable {
    private String game,username,comment;
    private Date commentedOn;

    public String getGame() {
        return game;
    }

    public String getUsername() {
        return username;
    }

    public String getComment() {
        return comment;
    }

    public Date getCommentedOn() {
        return commentedOn;
    }

    public Comment  (String game, String username, String comment, Date commentedOn) {
        this.game = game;
        this.username = username;
        this.comment = comment;
        this.commentedOn = commentedOn;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "game='" + game + '\'' +
                ", username='" + username + '\'' +
                ", comment=" + comment +
                ", commentedOn=" + commentedOn +
                '}';
    }

}
